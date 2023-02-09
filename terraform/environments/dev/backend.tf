terraform {
  backend "gcs" {
    # Uncomment below and specify a GCS bucket for TF state.
    bucket = "deviceconnect-376513-tfstate"
    prefix = "env/dev"
  }
}
