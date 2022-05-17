#!/usr/bin/env bash

# description:
# this helper script executes ansible-playbook against the inventory produced by the
# script ./ansible/tf_to_inv.sh.
# variables:
# ANSIBLE_CMD: the ansible command to run (default 'ansible-playbook')
# INVENTORY: the inventory file or script to use (default './ansible/tf_to_inv.sh')
# INSTALL_REQS: if 1, uses ansible-galaxy to install 

ANSIBLE_CMD=${ANSIBLE_CMD:-ansible-playbook}

INVENTORY=${INVENTORY:-./ansible/tf_to_inv.sh}

# disable host checking (fixme)
export ANSIBLE_SSH_ARGS="-o ControlMaster=auto -o ControlPersist=60s -o UserKnownHostsFile=/dev/null"
export ANSIBLE_CONFIG=ansible/ansible.cfg

if [[ ${INSTALL_REQS:-0} -eq 1 ]]; then
    # install prereqs first
    ansible-galaxy install -r ./ansible/requirements.yml
fi

echo "* Using inventory script: ${INVENTORY}"

# ansible vault notes (if applicable, i.e. a vaultfile exists):
# to use the vault, you must add the following when invoking ansible-playbook:
#   --extra-vars @vaultfile.yml.enc --vault-password-file .secrets/vault-pass
# to edit the vault file, use the following command:
#   ansible-vault edit --vault-password-file .secrets/vault-pass vaultfile.yml

${ANSIBLE_CMD} -i ${INVENTORY} "$@"
