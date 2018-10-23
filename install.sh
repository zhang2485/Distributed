#!/usr/bin/env bash
ansible-playbook -i ansible/hosts ansible/install.yml -f 10 --ask-become-pass
