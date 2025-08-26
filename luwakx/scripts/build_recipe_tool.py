#!/usr/bin/env python3
import argparse
import subprocess
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Map actions to script filenames and required args
action_map = {
    'format_standard': {
        'script': 'format_standard_tags.py',
        'args': ['--input', '--output']
    },
    'format_private': {
        'script': 'format_private_tags.py',
        'args': ['--input', '--output']
    },
    'make_basic_profile': {
        'script': 'make_deid_basic_profile_recipe.py',
        'args': ['--input', '--output']
    },
    'make_safe_private': {
        'script': 'make_deid_safe_private_tags_recipe.py',
        'args': ['--input', '--output']
    },
    'make_other_profile': {
        'script': 'make_other_profiles.py',
        'args': ['--input', '--output', '--profile']
    },
    # Add more actions/scripts as needed
}

def run_script(script_name, script_args):
    script_path = os.path.join(SCRIPT_DIR, script_name)
    if not os.path.exists(script_path):
        print(f"Script not found: {script_path}")
        sys.exit(1)
    cmd = [sys.executable, script_path] + script_args
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"Error: Script {script_name} failed.")
        sys.exit(result.returncode)

def main():
    parser = argparse.ArgumentParser(description="Run DICOM recipe/formatting tools.")
    parser.add_argument('--format-standard', action='store_true', help='Run format_standard_tags.py')
    parser.add_argument('--format-private', action='store_true', help='Run format_private_tags.py')
    parser.add_argument('--make-basic-profile', action='store_true', help='Run make_deid_basic_profile_recipe.py')
    parser.add_argument('--make-safe-private', action='store_true', help='Run make_deid_safe_private_tags_recipe.py')
    parser.add_argument('--make-other-profile', action='store_true', help='Run make_other_profiles.py')
    parser.add_argument('--input', type=str, help='Input file path')
    parser.add_argument('--output', type=str, help='Output file path')
    args = parser.parse_args()

    # Determine which actions to run
    for action, info in action_map.items():
        if getattr(args, action.replace('-', '_')):
            script_args = []
            for arg in info['args']:
                val = getattr(args, arg.lstrip('-'))
                if val:
                    script_args.extend([arg, val])
            run_script(info['script'], script_args)

if __name__ == '__main__':
    main()
