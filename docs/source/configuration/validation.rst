Configuration Validation
========================

Overview
--------

Configuration validation ensures that all settings declared in YAML files
(e.g., ``agents.yaml``, ``databases.yaml``, ``producers.yaml``) are
consistent, complete, and valid before the framework instantiates the components.

The framework leverages **Pydantic models** to enforce types, constraints,
and custom validation logic for each configuration entity.

---

How validation works
--------------------

1. Each configuration file is parsed into Python dictionaries.
2. Each dictionary is passed to the corresponding **Pydantic model**.
3. Pydantic automatically:
   - checks type consistency for each field
   - applies default values where specified
   - executes **field validators** for custom logic
4. Any validation error raises a **ValidationError**, preventing the framework
   from starting with invalid configuration.

---

Example: ``PathFileConfig``
---------------------------

Below is an example of a Pydantic model used to validate a file path configuration:

.. code-block:: python

   from pathlib import Path
   from pydantic import BaseModel, field_validator
   from typing import Tuple, Optional

   class PathFileConfig(BaseModel):
       name: str
       path: Path
       cursor: int = 0
       id: Optional[int] = None

       @field_validator('name')
       def validate_name(cls, value):
           if value is None:
               raise ValueError("Name cannot be None")
           return value

       @field_validator('path')
       def validate_path(cls, value):
           if value is None:
               raise ValueError("Path cannot be None")
           if not isinstance(value, Path):
               raise ValueError("Path must be a Path object")
           if not value.exists():
               raise ValueError("Path does not exist")
           return value

       @field_validator('id')
       def validate_inode(cls, value):
           if value is not None and not isinstance(value, int):
               raise ValueError("id must be an integer")
           return value

---

Key points
----------

- **Type safety:** ensures fields are of the expected type (e.g., ``Path``, ``int``).
- **Custom validation:** field validators allow complex rules like checking
  file existence or mandatory values.
- **Early error detection:** prevents misconfigured agents, producers, or databases
  from running.
- **Extensibility:** you can define custom validators for any new configuration
  model added to the framework.

---

Best practices
--------------

- Keep validation logic **in the model**, not in the runtime flow.
- Use **descriptive error messages** to help debugging misconfigurations.
- Validate **all YAML files at startup** to catch errors early.
