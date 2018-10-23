#!/usr/bin/env bash
ansible-playbook -i ansible/hosts ansible/shutdown.yml -f 10 --ask-become-pass
