# keda-observation

Pipeline de streaming événementiel (Wikipedia → Kafka → PostgreSQL) avec autoscaling piloté par la charge réelle (KEDA), déploiement continu en GitOps (ArgoCD), et observabilité (Prometheus/Grafana).

Ce document explique **quoi existe, où, et pourquoi**, avec le niveau de détail nécessaire pour reprendre le projet après une longue pause.

---

## Vue d'ensemble

```
Wikipedia (flux public temps réel)
        │
        ▼
   producer (Go)  ──publie──▶  Redpanda (Kafka)  ──lu par──▶  consumer (Go)  ──insère──▶  PostgreSQL
                                      │                              │
                                      │ (lag surveillé)              │ (métriques exposées)
                                      ▼                              ▼
                                    KEDA  ──scale──▶  Deployment consumer
                                                              │
                                                              ▼
                                                        Prometheus / Grafana
```

Tout est déployé sur un cluster kubeadm (3 VM Hetzner), géré en GitOps via ArgoCD : le contenu de ce repo est la seule source de vérité, rien ne se modifie à la main dans le cluster.

---

## Arborescence du repo

```
apps/
├── producer/           code Go : lit Wikipedia, publie dans Kafka
│   ├── main.go
│   ├── go.mod
│   └── Dockerfile
└── consumer/            code Go : lit Kafka, écrit dans Postgres, expose /metrics
    ├── main.go
    ├── go.mod
    └── Dockerfile

k8s/
├── base/                 la définition complète du projet (tout, sans exception)
│   ├── kustomization.yaml    liste les 13 fichiers ci-dessous
│   ├── namespace.yaml
│   ├── redpanda/service.yaml
│   ├── redpanda/statefulset.yaml
│   ├── postgres/secret.yaml
│   ├── postgres/service.yaml
│   ├── postgres/statefulset.yaml
│   ├── producer/configmap.yaml
│   ├── producer/deployment.yaml
│   ├── consumer/configmap.yaml
│   ├── consumer/deployment.yaml
│   ├── consumer/service.yaml
│   ├── keda/scaledobject.yaml
│   └── monitoring/consumer-servicemonitor.yaml
│
└── overlays/prod/
    └── kustomization.yaml    référence base/, patch le tag des images

argocd/
└── application.yaml      objet Application ArgoCD, pointe vers k8s/overlays/prod
```

---

## Ce qui est installé sur le cluster, mais qui n'appartient pas à ce repo

Ces briques sont des infrastructures partagées du cluster, installées une fois manuellement, indépendantes de ce projet précis :

- **local-path-provisioner** : fournit la StorageClass `local-path`, utilisée par Postgres et Redpanda pour obtenir un vrai disque sur le node.
- **KEDA** (namespace `keda`) : installé via le manifest officiel `keda-2.15.1.yaml` (téléchargé depuis le repo GitHub officiel de KEDA, pas écrit par nous). Ajoute au cluster les types `ScaledObject`, `ScaledJob`, `TriggerAuthentication`, et le pod `keda-operator` qui les surveille en permanence.
- **kube-prometheus-stack** (namespace `monitoring`) : déjà présent sur ce cluster avant ce projet, installé via Helm (release `prometheus`). Fournit Prometheus, Grafana, kube-state-metrics, node-exporter, et le type `ServiceMonitor`.
- **ArgoCD** (namespace `argocd`) : déjà présent sur ce cluster avant ce projet.

Nodes labellisés manuellement (une fois, en CLI, jamais versionné) :
```
kubectl label node k8s-worker-1 workload=data-plane   (Redpanda, Prometheus, Grafana)
kubectl label node k8s-worker-2 workload=app-plane    (Postgres, producer, consumer)
```

---

## Phase 1 : bootstrap GitOps (fait une seule fois)

C'est la seule action manuelle de tout le projet. Après ça, plus rien ne se fait à la main.

```
kubectl apply -f https://raw.githubusercontent.com/<user>/keda-observation/main/argocd/application.yaml
```

Déroulé :

```
Kubernetes crée l'objet Application "keda-observation" dans le namespace argocd
        │
        ▼
argocd-application-controller voit ce nouvel objet
        │
        │ lit spec.source.repoURL et spec.source.path (k8s/overlays/prod)
        ▼
argocd-repo-server clone le repo, se place dans k8s/overlays/prod
        │
        │ détecte un kustomization.yaml, invoque Kustomize (intégré à ArgoCD)
        ▼
Kustomize lit k8s/overlays/prod/kustomization.yaml
        │
        ├── resources: ../../base
        │       → va lire k8s/base/kustomization.yaml
        │       → assemble les 13 fichiers listés dedans
        │
        └── images: [...]
                → scanne les objets assemblés
                → trouve les containers dont l'image correspond
                  à ghcr.io/<user>/producer et .../consumer
                → remplace leur tag par newTag (patché par la CI plus tard)
        │
        ▼
Résultat : un flux YAML final, 13 objets, images à jour
        │
        ▼
argocd-application-controller applique ce résultat sur l'API Kubernetes
        │
        ▼
Les contrôleurs natifs (Deployment, StatefulSet) créent les pods réels
sur les nodes, selon les nodeSelector définis dans chaque manifest
```

Ensuite, en continu : ArgoCD compare l'état du repo à l'état réel du cluster toutes les quelques minutes, et corrige automatiquement toute différence (`selfHeal: true`), y compris si quelqu'un modifie un objet à la main avec `kubectl edit`.

---

## Phase 2 : pourquoi base/ et overlays/prod/ sont séparés

`base/kustomization.yaml` liste la totalité des objets du projet, sans exception, même ceux qui ne contiennent aucune image (namespace, secret, ScaledObject, ServiceMonitor). Son seul rôle est de garantir que **tout** existe dans le cluster.

`overlays/prod/kustomization.yaml` référence `base/` en entier, puis applique un patch supplémentaire, limité au tag des deux images. C'est le seul fichier que la CI modifiera automatiquement au moment d'un déploiement (`kustomize edit set image ...`), pour ne jamais toucher au reste du projet.

ArgoCD pointe précisément sur `k8s/overlays/prod`, jamais sur `k8s/` en général : les deux dossiers contiennent chacun un `kustomization.yaml`, et sans préciser lequel est le point d'entrée, ArgoCD tenterait de traiter les deux comme des sources indépendantes, ce qui produirait deux définitions concurrentes des mêmes objets.

---

## Phase 3 : flux de données en continu

```
stream.wikimedia.org/v2/stream/recentchange  (flux public externe)
        │
        │ requête HTTP GET, header Accept: text/event-stream
        │ (fonction consumeStream, apps/producer/main.go)
        ▼
pod producer (1 réplique fixe, jamais scalé — plusieurs instances
dupliqueraient les mêmes événements dans Kafka)
        │
        │ parse chaque ligne JSON en struct WikiEvent
        │ filtre : event.Wiki == WIKI_FILTER ("frwiki")
        │ throttle : garde 1 événement sur SAMPLE_RATE (10)
        │ publie via kafka-go, cible KAFKA_BROKERS ("redpanda:9092")
        ▼
Service redpanda (headless, port 9092) → pod redpanda-0
        │
        │ stocke le message dans le topic KAFKA_TOPIC ("wikipedia-events")
        ▼
pod consumer (1 à N répliques, pilotées par KEDA)
        │
        │ lit via kafka-go, GroupID = KAFKA_GROUP_ID
        │ ("wikipedia-consumer-group" — si plusieurs pods consumer
        │ partagent ce même GroupID, Kafka répartit automatiquement
        │ les messages entre eux)
        │
        │ parse le JSON, insère dans Postgres via pool.Exec(...)
        │ connexion : DATABASE_URL (Secret postgres-credentials,
        │ injecté par env.valueFrom.secretKeyRef)
        ▼
Service postgres (headless, port 5432) → pod postgres-0
        │
        │ la table wiki_events est créée au premier démarrage du
        │ consumer (fonction ensureSchema, CREATE TABLE IF NOT EXISTS)
        ▼
Disque persistant (storageClassName local-path), monté sur
/var/lib/postgresql/data — survit aux redémarrages du pod
```

---

## Phase 4 : boucle de scaling KEDA

L'objet `ScaledObject` (`k8s/base/keda/scaledobject.yaml`) définit :
- `scaleTargetRef.name: consumer` — quel Deployment scaler
- `minReplicaCount: 1`, `maxReplicaCount: 8` — les bornes
- `triggers[0].metadata.bootstrapServers` — adresse complète (FQDN) de Redpanda, `redpanda.keda-observation.svc.cluster.local:9092`, nécessaire car `keda-operator` tourne dans le namespace `keda`, différent de `keda-observation`
- `lagThreshold: "20"` — la cible visée par message en attente

```
keda-operator a détecté cet objet dès sa création, et créé
automatiquement un HorizontalPodAutoscaler nommé keda-hpa-consumer-scaler
        │
        ▼
Toutes les 15 secondes (pollingInterval) :
        │
        │ keda-operator interroge Redpanda : quel est le lag actuel
        │ du groupe wikipedia-consumer-group ?
        │
        │ Redpanda répond un nombre, par exemple 85
        ▼
keda-operator expose ce nombre via une API de métriques externes
        │
        ▼
Le HPA (composant standard de Kubernetes, pas créé par nous) lit ce
nombre et calcule :
        replicas = lag_actuel / lagThreshold = 85 / 20 ≈ 5
        (borné entre minReplicaCount et maxReplicaCount)
        │
        ▼
Le HPA modifie spec.replicas du Deployment consumer
        │
        ▼
Le contrôleur Deployment (natif Kubernetes) crée ou supprime des pods
consumer pour atteindre ce nombre. Chaque nouveau pod rejoint
automatiquement le même GroupID Kafka, qui répartit alors la charge
entre tous les pods actifs.
```

Vérification manuelle du lag réel :
```
kubectl exec -it redpanda-0 -n keda-observation -- rpk group describe wikipedia-consumer-group
```

---

## Phase 5 : observabilité

Le container consumer expose ses métriques sur `http://localhost:9090/metrics` (package `promhttp`, fichier `apps/consumer/main.go`) : `wiki_consumer_events_total`, `wiki_consumer_events_failed_total`, `wiki_consumer_insert_duration_seconds`.

Le Service `consumer` (`k8s/base/consumer/service.yaml`) expose ce port sous le nom `metrics`, sans effet tant que rien ne l'interroge.

L'objet `ServiceMonitor` (`k8s/base/monitoring/consumer-servicemonitor.yaml`) porte le label `release: prometheus`, requis car le Prometheus de ce cluster n'accepte que les ServiceMonitor portant ce label précis dans son `serviceMonitorSelector`.

```
Prometheus Operator (pod à part, namespace monitoring, différent de
Prometheus lui-même) surveille l'API Kubernetes en permanence
        │
        │ voit apparaître le ServiceMonitor "consumer"
        │ vérifie que son label release: prometheus correspond à ce
        │ que l'objet Prometheus (CRD) attend
        ▼
Prometheus Operator génère la configuration de scrape correspondante,
l'injecte dans un Secret lu par Prometheus, puis force un rechargement
à chaud (endpoint /-/reload)
        │
        ▼
Prometheus scrape http://consumer.keda-observation.svc.cluster.local:9090/metrics
toutes les 15 secondes, et conserve l'historique dans le temps

Prometheus scrape également kube-state-metrics, qui expose l'état
du HPA keda-hpa-consumer-scaler (nombre de réplicas courant et désiré)
        │
        ▼
Grafana interroge Prometheus pour construire les graphiques : débit
d'événements, latence d'insertion, et nombre de pods consumer dans
le temps, corrélé au lag Kafka
```

---

## Résolution des noms internes utilisés dans le projet

| Appelant | Cible | Port | Remarque |
|---|---|---|---|
| producer | redpanda | 9092 | même namespace, nom court suffisant |
| consumer | redpanda | 9092 | idem |
| consumer | postgres.keda-observation.svc.cluster.local | 5432 | FQDN, fourni via Secret |
| keda-operator | redpanda.keda-observation.svc.cluster.local | 9092 | FQDN obligatoire, namespace différent (keda) |
| Prometheus | consumer.keda-observation.svc.cluster.local | 9090 | via le ServiceMonitor |
| Grafana | prometheus | — | même namespace (monitoring) |
| ArgoCD | github.com/&lt;user&gt;/keda-observation | 443 | HTTPS, externe au cluster |

---

## Points d'attention pour une reprise future

- Les images Docker doivent être construites avec `--platform linux/amd64` : les VM Hetzner sont en x86_64, un build fait depuis un Mac Apple Silicon produit par défaut une image `arm64` incompatible.
- Un changement dans la commande de démarrage d'un `StatefulSet` (Redpanda, Postgres) n'entraîne pas de redémarrage automatique du pod existant, contrairement à un `Deployment`. Il faut supprimer le pod manuellement (`kubectl delete pod ...`) pour qu'il reparte avec la nouvelle définition.
- Le secret `postgres-credentials` est stocké en clair dans le repo, à des fins de démonstration uniquement. Une vraie mise en production demanderait un mécanisme dédié (sealed-secrets ou external-secrets).
- `SAMPLE_RATE` (dans `k8s/base/producer/configmap.yaml`) contrôle le volume d'événements réellement publiés. Le réduire ou le supprimer temporairement permet de générer plus de charge pour observer le scaling KEDA en action.