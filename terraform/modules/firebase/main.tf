/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

# Enabling a firebase project

resource "google_app_engine_application" "firebase_init" {
  # Only execute this module when global var firebase_init set as "true"
  # NOTE: the Firebase can only be initialized once (via App Engine).
  count = var.firebase_init ? 1 : 0

  provider      = google-beta
  project       = var.project_id
  location_id   = var.firestore_region
  database_type = "CLOUD_FIRESTORE"
}

resource "google_storage_bucket" "firestore-backup-bucket" {
  name          = "${var.project_id}-firestore-backup"
  location      = var.storage_multiregion
  storage_class = "NEARLINE"

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 356
    }
    action {
      type = "Delete"
    }
  }
}
