"""Authentication commands: login, logout, context."""
from __future__ import annotations

import getpass
import sys
from typing import Any

from kanoniv.cli.config import (
    save_credentials,
    clear_credentials,
    get_context,
    resolve_api_url,
)
from kanoniv.cli.output import print_json


def cmd_login(args: Any) -> None:
    """Store API key credentials."""
    api_key = args.api_key_flag or _prompt_api_key()
    api_url = resolve_api_url(args.api_url_flag)

    save_credentials(api_key, api_url)

    if args.format == "json":
        print_json({"status": "ok", "api_url": api_url})
    else:
        sys.stdout.write(f"Credentials saved for {api_url}\n")


def cmd_logout(args: Any) -> None:
    """Remove stored credentials."""
    clear_credentials()

    if args.format == "json":
        print_json({"status": "ok"})
    else:
        sys.stdout.write("Credentials removed.\n")


def cmd_context(args: Any) -> None:
    """Show the current authentication context."""
    ctx = get_context()

    if args.format == "json":
        print_json(ctx)
    else:
        sys.stdout.write(f"API URL:  {ctx['api_url']}\n")
        sys.stdout.write(f"API Key:  {ctx['api_key']}\n")
        sys.stdout.write(f"File:     {ctx['credentials_file']}\n")


def _prompt_api_key() -> str:
    """Interactively prompt for the API key."""
    try:
        key = getpass.getpass("API Key: ")
    except (EOFError, KeyboardInterrupt):
        sys.stderr.write("\nAborted.\n")
        sys.exit(1)
    if not key.strip():
        sys.stderr.write("error: API key cannot be empty\n")
        sys.exit(1)
    return key.strip()
