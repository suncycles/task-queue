# Task registry: maps "module.ClassName.method" -> callable.
#
# IMPORTANT: Workers must explicitly import the tasks module before starting
# the BRPOP loop. There is no magic auto-discovery — this is intentional.
# Auto-discovery via __import__ makes import errors silent and hard to debug.
#
# Correct worker startup order:
#   1. import tasks.example_tasks   # populates registry
#   2. worker.run()                 # starts BRPOP loop

_registry: dict[str, callable] = {}


def task(fn):
    """Decorator that registers a function under its fully-qualified name."""
    key = f"{fn.__module__}.{fn.__qualname__}"
    _registry[key] = fn
    return fn


def get_task(name: str):
    if name not in _registry:
        registered = ", ".join(sorted(_registry.keys())) or "(none)"
        raise KeyError(
            f"Unknown task: '{name}'.\n"
            f"Did you import the module that defines it?\n"
            f"Currently registered tasks: {registered}"
        )
    return _registry[name]


def list_tasks() -> list[str]:
    return sorted(_registry.keys())