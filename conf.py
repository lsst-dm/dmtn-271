"""Sphinx configuration.

To learn more about the Sphinx configuration for technotes, and how to
customize it, see:

https://documenteer.lsst.io/technotes/configuration.html
"""

from documenteer.conf.technote import *  # noqa: F401, F403

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'networkx': ('https://networkx.org/documentation/stable', None),
    'lsst.resources': ('https://pipelines.lsst.io/v/weekly', None),
    'lsst.daf.butler': ('https://pipelines.lsst.io/v/weekly', None),
    'lsst.pipe.base': ('https://pipelines.lsst.io/v/weekly', None),
    'lsst.ctrl.mpexec': ('https://pipelines.lsst.io/v/weekly', None),
    }
