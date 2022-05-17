# ---------------------------------------------------------------------------------------------------
# --- VM config
# ---------------------------------------------------------------------------------------------------

variable "node_image" {
  default = "debian-cloud/debian-10"
}

variable "virtual_machines" {
  default = {
    vm = {
      // the class + specs of the machine; either choose an existing class or
      // formulate a custom machine class string. for example, for an e2 machine
      // with 6 CPUs and ~3GB of RAM, you'd use "e2-custom-6-3072"
      // more info: https://cloud.google.com/compute/docs/instances/creating-instance-with-custom-machine-type#gcloud
      machine_type = "e2-highmem-8"

      tags = ["http-server", "https-server"]
      role = "standard" // the VM will be placed in this ansible host group (default: "ungrouped")

      // optional attributes:
      // - disk_size_gb(int):
      //   size of the startup disk in gigabytes (default: 30)
      // - node_image(str):
      //   name of the image to deploy (default: node_image, var)
      // - external_ip(str):
      //   an allocated static IP to bind to this instance (default: assigns an ephemeral IP)
    }
  }
}

resource "google_compute_instance" "nodes" {
  for_each = var.virtual_machines

  name         = "${var.resource_prefix}${replace(each.key, "_", "-")}"
  machine_type = each.value.machine_type
  tags         = try(each.value.tags, [])

  boot_disk {
    initialize_params {
      image = try(each.value.node_image, var.node_image)
      size  = try(each.value.disk_size_gb, 30)
    }
  }

  metadata = {
    role = try(each.value.role, "ungrouped")
  }

  network_interface {
    network    = google_compute_network.network.id
    subnetwork = google_compute_subnetwork.subnetwork.id

    access_config {
      nat_ip = try(each.value.external_ip, null)
    }
  }
}
