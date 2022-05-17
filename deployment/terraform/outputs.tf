output "instance_ip_addr" {
  // reference: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/compute_instance#network_interface.0.access_config.0.nat_ip
  value = {
    for value in google_compute_instance.nodes :
    value.name => value.network_interface.0.access_config.0.nat_ip
  }
}
