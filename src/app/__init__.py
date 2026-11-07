"""The composition root: settings, logging, resources, modules, the kernel.

The only place that knows every layer: it turns the settings into adapters
(`factories`) and hands them, as ports, to the business modules
(`bootstrap`).
"""
