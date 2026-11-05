# D3 (account and billing) is still open, so the project and billing account have no
# defaults: every apply and destroy names them explicitly (a *.tfvars file, never committed).

variable "project_id" {
  description = "The Google Cloud project for S5.1 (a dedicated project; teardown targets only it)."
  type        = string
}

variable "billing_account" {
  description = "Billing account ID (XXXXXX-XXXXXX-XXXXXX) the budget alert watches."
  type        = string
}

variable "admin_cidrs" {
  description = "CIDRs allowed to reach the GKE control plane (the operators' IPs). D7: deploys are manual."
  type        = list(string)
}

variable "alert_emails" {
  description = "Who receives the budget alerts (both owners)."
  type        = list(string)
}

# D2: asia-south1-a.
variable "region" {
  description = "Region of the static IP, the registry and the subnet."
  type        = string
  default     = "asia-south1"
}

variable "zone" {
  description = "Zone of the zonal cluster and its node pools."
  type        = string
  default     = "asia-south1-a"
}

variable "cluster_name" {
  description = "GKE cluster name; the kube context is gke_<project>_<zone>_<name>."
  type        = string
  default     = "reliefgrid"
}

# Sizes from the plan (§2): 3 data nodes + 1 infra node = 10 vCPU. If the account caps
# vCPUs at 8 (free trial), set infra_machine_type = "e2-standard-2": 4 x e2-standard-2.
variable "data_machine_type" {
  description = "Machine type of the three data nodes (role=data)."
  type        = string
  default     = "e2-standard-2"
}

variable "infra_machine_type" {
  description = "Machine type of the infra node (role=infra, tainted). e2-standard-2 under an 8-vCPU cap."
  type        = string
  default     = "e2-standard-4"
}

# D1: a budget alert (planned at about $25). Budgets alert; they never cap spending. The
# amount is in the billing account's currency, which may be INR, so neither has a default:
# both are set in the tfvars once D3 (the billing account) is settled.
variable "budget_amount" {
  description = "Budget alert amount, in budget_currency (whole units)."
  type        = number
}

variable "budget_currency" {
  description = "ISO 4217 code of the billing account's currency (e.g. INR or USD); any other is rejected by the API."
  type        = string
}

variable "github_repository" {
  description = "owner/name of the only repository whose main branch may use the CI identity."
  type        = string
  default     = "ParthMalhotra07/ReliefGrid"
}

# Node upgrades must not drain a node during the demo window (plan §2).
variable "maintenance_exclusion_start" {
  description = "RFC 3339 start of the no-upgrade window."
  type        = string
  default     = "2026-11-12T00:00:00Z"
}

variable "maintenance_exclusion_end" {
  description = "RFC 3339 end of the no-upgrade window."
  type        = string
  default     = "2026-11-21T00:00:00Z"
}
