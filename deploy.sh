#!/usr/bin/env bash
ansible-playbook -i ansible/hosts ansible/main.yml -f 10 --ask-become-pass
