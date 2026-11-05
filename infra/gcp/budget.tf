# D1: a budget alert on this project at 50/90/100 %. It alerts; it never stops spending,
# which is why bring-up is per session and gke-teardown.sh checks nothing is left.
# Needs billing.budgets.create on the billing account (may be a manual grant).

data "google_project" "this" {
  project_id = var.project_id
}

resource "google_monitoring_notification_channel" "budget" {
  for_each     = toset(var.alert_emails)
  display_name = "ReliefGrid budget: ${each.value}"
  type         = "email"
  labels = {
    email_address = each.value
  }
  depends_on = [google_project_service.enabled]
}

resource "google_billing_budget" "s51" {
  billing_account = var.billing_account
  display_name    = "ReliefGrid S5.1 ${var.project_id}"

  budget_filter {
    projects = ["projects/${data.google_project.this.number}"]
  }

  amount {
    specified_amount {
      currency_code = var.budget_currency
      units         = tostring(var.budget_amount)
    }
  }

  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.9
  }
  threshold_rules {
    threshold_percent = 1.0
  }

  all_updates_rule {
    monitoring_notification_channels = [for c in google_monitoring_notification_channel.budget : c.id]
    disable_default_iam_recipients   = false
  }

  depends_on = [google_project_service.enabled]
}
