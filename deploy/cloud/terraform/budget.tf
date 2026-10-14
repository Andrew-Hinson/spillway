# Optional spending alert. AWS Budgets only notifies: it stops nothing, and its
# figures lag actual spend by hours. `make cloud-down` is what stops the cost.
resource "aws_budgets_budget" "spillway" {
  count = var.budget_email == "" ? 0 : 1

  name         = var.cluster_name
  budget_type  = "COST"
  limit_amount = tostring(var.budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_email]
  }
}
