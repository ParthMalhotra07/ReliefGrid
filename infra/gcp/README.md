# infra/gcp: the S5.1 GKE session

Terraform for a zonal GKE Standard cluster (Dataplane V2; 3 `role=data` nodes and 1 tainted
`role=infra` node), its network and static ingress IP, an Artifact Registry repository, the node
and CI identities (Workload Identity Federation, main only, no keys) and a budget alert.
The cluster runs only during a session: apply, deploy, demo, tear down.

Nothing here is applied by CI. CI only checks it offline (`make gcp-validate`: fmt, validate and
the shape tests in `scripts/gke_test.go`).

## Once per project: the state bucket

Made by hand, outside this configuration, so `terraform destroy` never removes its own state:

```sh
gcloud storage buckets create gs://<state bucket> --project=<project> --location=<region> \
  --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update gs://<state bucket> --versioning
# members: only the two owners (roles/storage.objectAdmin on the bucket)
```

## A session

1. `cp terraform.tfvars.example ~/s51.tfvars` and fill it in. Set `budget_currency` to the billing
   account's currency and `budget_amount` in it (D1/D3). Keep the file outside the repository.
2. `terraform -chdir=infra/gcp init -backend-config="bucket=<state bucket>"`
3. Check prices and quotas (CPUS, IN_USE_ADDRESSES) first, then
   `terraform -chdir=infra/gcp apply -var-file=~/s51.tfvars`.
4. Set the repository variables `GCP_WORKLOAD_IDENTITY_PROVIDER` (output `wif_provider`),
   `GCP_CI_SERVICE_ACCOUNT` (`ci_service_account`) and `GCP_ARTIFACT_REGISTRY` (`registry`).
   Then run the `ci` workflow on main (workflow_dispatch). It copies each image by digest into the
   registry, and its `image-digests` artifact is the deploy's input.
5. `IMAGE_DIGESTS=… OPERATOR_TOKEN_FILE=… make gke-deploy`. Use `GKE_DEPLOY_APP=false` for storage
   only. The app also needs `DISPATCHER_TOKEN_FILE`, `ALLOWED_CIDRS` and A's
   `values-app-gke.yaml` (#115).
6. `GKE_TFVARS=~/s51.tfvars STATE_BUCKET=<state bucket> make gke-teardown`, then delete the three
   repository variables.

## Teardown checklist (A5.2)

- [ ] `make gke-teardown` asked for the project ID and targeted only that project and cluster
- [ ] it waited until no PVC, PV or LoadBalancer Service was left before `terraform destroy`
- [ ] every listing printed `none` (only the state bucket and the `default` network remain)
- [ ] the three `GCP_*` repository variables are deleted
- [ ] the next day, the billing report for the project shows ≈ 0 after the teardown time
