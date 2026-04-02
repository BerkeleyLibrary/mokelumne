#!/usr/bin/env python3
"""Initialize a local .env file from example.env, generating missing secrets."""

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_ENV = ROOT / "example.env"
DOT_ENV = ROOT / ".env"


def generate_hex_secret() -> str:
    return subprocess.check_output(["openssl", "rand", "-hex", "32"], text=True).strip()


def generate_fernet_key() -> str:
    from cryptography.fernet import Fernet

    return Fernet.generate_key().decode()


GENERATORS = {
    "AIRFLOW__API_AUTH__JWT_SECRET": generate_hex_secret,
    "AIRFLOW__CORE__FERNET_KEY": generate_fernet_key,
    "OIDC_CLIENT_SECRET": generate_hex_secret,
}


def main() -> None:
    if DOT_ENV.exists():
        print(f".env already exists at {DOT_ENV}")
        print("Delete it first if you want to regenerate.")
        sys.exit(1)

    shutil.copy(EXAMPLE_ENV, DOT_ENV)

    text = DOT_ENV.read_text()
    for var, generate in GENERATORS.items():
        placeholder = f"{var}="
        # Only fill in empty values (line ends right after '=')
        if f"{placeholder}\n" in text or text.endswith(placeholder):
            value = generate()
            text = text.replace(f"{placeholder}\n", f"{placeholder}{value}\n", 1)
            if text.endswith(placeholder):
                text = text[: -len(placeholder)] + f"{placeholder}{value}"
            print(f"Generated {var}")
        else:
            print(f"Skipped {var} (already has a value)")

    DOT_ENV.write_text(text)
    print(f"\nWrote {DOT_ENV}")


if __name__ == "__main__":
    main()
