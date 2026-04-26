import os

def pytest_configure(config):
    """This is a pytest hook that is called automatically at startup.
    """

    # Load .env.local from the project root into os.environ before tests run.
    # This allows us to keep secrets like API keys out of version control
    # and global environment variableswhile still making them available to tests.
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_file = os.path.join(root, ".env.local")
    if not os.path.exists(env_file):
        return
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())
