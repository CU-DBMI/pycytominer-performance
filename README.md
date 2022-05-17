# Terraform + Ansible GCP Deployment Template

This repo contains terraform and ansible configuration for setting up a single
VM with a networking layer.

## Prerequisities and Initial Notes

You'll need to install [Terraform](https://www.terraform.io/) and
[Ansible](https://www.ansible.com/) prior to using the scripts in this repo.
You'll also need a GNU environment, including bash or a bash-compatible shell.

While it's not required, you'll probably want to install the Google Cloud CLI
for your platform. Follow the [CLI installation
instructions](https://cloud.google.com/sdk/docs/install-sdk) and be sure to log
in via `gcloud auth login` once it's installed.

In order to provsion the resources specified in this repo to GCP, you'll need a
service key with Compute Engine create permissions. You should obtain a service
key with the necessary permissions, name it `sa.json`, and put it in
`./deployment/.secrets`.

## Usage

Run `./deployment/provision.sh` to first run terraform to provision the
infrastructure, then ansible to deploy software to it. To perform an unattended
fresh reinstall, you can pass the `-d` (for "destroy") and `-x` (automatically
accept) flags to the script, i.e.:

```
./deployment/provision.sh -x -d
```

After a few minutes, you should have a provisioned VM. The state of this VM and
any other created resources (e.g., network configuration) are stored in an
untracked file called `./deployment/terraform.tfstate`; don't check this file in
or otherwise share it as it can contain sensitive information.

The Ansible playbook at `./deployment/ansible/setup.yml` will also be run
against the inventory of VMs created by Terraform, but initially it does nothing
but check that the hosts are online and make them echo a greeting. Feel free to
modify it to your needs.

### Connecting to the VM

Assuming you have the Google Cloud (aka "gcloud") CLI installed, you can SSH
into your machine with the following command:

```
gcloud compute ssh --project=cuhealthai-sandbox --zone=us-central1-a test-sample-vm
```

### Cleaning Up

When you're done using your resources and want to tear down the infrastructure,
the following command will destroy the resources and not recreate them (`-na`,
"no apply", skips the Terraform "apply" step that creates resources):

```
./deployment/provision.sh -x -d -na
```

## Configuration

First off, you'll find project-level variables specified in
`./deployment/terraform/config.tg`. Feel free to modify them in that file, or
override their values through [Terraform's many methods for specifying module
variable
values](https://www.terraform.io/language/values/variables#assigning-values-to-root-module-variables).

Most of the infrastructure specification lives in
`./deployment/terraform/machines.tf`; modify the `virtual_machines` variable to
your needs.

You'll find network configuration in `./deployment/terraform/network.tf`; if you
need to open additional ports on your VM, you'd add a tag description there,
then add the tag to your VM's list of tags. See "fw_http_tag" for an example, in
that case of opening port 80 to any client.

Finally, `./deployment/terraform/outputs.tf` allows you to specify information
you'd like to gather via Terraform that you'd want to persist to another module.
The current configuration produces a dictionary of GCP instance name to its
external IP address as the output. The values outputted via `outputs.tf` are
available as host-level variables in Ansible; for example, in the current config
"{{ vars.outputs.instance_ip_addr['test-vm-sample-vm'] }}" would resolve to the
external IP of the `test-vm-sample-vm` instance.