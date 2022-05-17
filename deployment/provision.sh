#!/usr/bin/env bash

export ANSIBLE_PLAYBOOK=${ANSIBLE_PLAYBOOK:-ansible/setup.yml}
export INVENTORY=${INVENTORY:-./ansible/tf_to_inv.sh}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

DO_DESTROY=0
DO_APPLY=1
NO_TF=0
NO_ANSIBLE=0
AUTO_APPROVE=""

function exit_w_msg {
  echo "$1"
  exit ${2:-1}
}

# we need to be in the script dir for relative references to work, so let's
# just cd there
cd $SCRIPT_DIR

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
    -h|--help)
      echo "Usage: $0 [-d|--destroy] [-a|--approve] [-na|--no-apply] [-n|--no-tf] <REST>"
      echo "  -d|--destroy    destroys any existing terraform infrastructure"
      echo "  -x|--approve    automatically approves any terraform confirmation requests"
      echo "  -na|--no-apply  skips the terraform apply step"
      echo "  -n|--no-tf      skips terraform entirely"
      echo "  <REST>          arguments are passed to ansible-playbook as-is"
      echo ""
      echo "Description:"
      echo "  This script is used to deploy an example VM to "
      echo "  Google Cloud, using terraform to set up infrastructure and ansible"
      echo "  to configure the deployed instance(s)."
      echo "  "
      echo "  The script first runs the contents of ./terraform as a module, then"
      echo "  runs an ansible playbook (default: ansible/setup_rke2.yml) to set up"
      echo "  the resources created by terraform. The ansible playbook is passed"
      echo "  an inventory built from the terraform VMs specified in ./terraform/machines.tf"
      echo "  and grouped by the 'role' attribute."
      echo ""
      exit 0
      ;;
    -d|--destroy)
      DO_DESTROY="1"
      shift
      ;;
    -x|--approve)
      AUTO_APPROVE="-auto-approve"
      shift
      ;;
    -na|--no-apply)
      DO_APPLY=0
      shift
      ;;
    -n|--no-tf) # skip anything tf-related
      NO_TF="1"
      shift
      ;;
    -nx|--no-ansible)
      NO_ANSIBLE=1
      shift
      ;;
    *)    # unknown option
      POSITIONAL+=("$1") # save it in an array for later
      shift
      ;;
  esac
done

(
  [[ ${NO_TF} == "1" ]] || (
      cd terraform
      ( [[ ${DO_DESTROY} != "1" ]] || terraform destroy ${AUTO_APPROVE} ) \
       && \
      ( [[ ${DO_APPLY} != "1" ]] || terraform apply ${AUTO_APPROVE} ) \
  )
) && (
  ! ${INVENTORY} >/dev/null 2>&1 \
    && exit_w_msg "* inventory has nonzero exit status (empty?), aborting" \
    || (
      [[ ${NO_ANSIBLE} == "1" ]] ||  \
      ./ansible_on_tfinv.sh ${ANSIBLE_PLAYBOOK}
    )
)
