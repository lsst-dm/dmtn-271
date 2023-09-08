from __future__ import annotations

import enum
import uuid
from abc import abstractmethod, ABC
from collections.abc import Callable, Iterable, Mapping, Sequence, Set
from types import EllipsisType
from typing import Any, TypeVar

import networkx as nx  # type: ignore[import]
from lsst.daf.butler import (
    ButlerConfig,
    DataCoordinate,
    DataId,
    DatasetRef,
    DimensionGraph,
    DimensionUniverse,
    LimitedButler,
)
from lsst.resources import ResourcePath, ResourcePathExpression


class QuantumGraphExpression:
    """Placeholder for a parsed expression that subsets a quantum graph.

    This is expected to include:

    - the standard set-theory operations (intersection, union, difference,
      symmetric difference, inversion);
    - literal sets of quanta and datasets;
    - range syntax for ancestors and descendants of quanta and datasets (see
      `PipelineGraphExpression`).

    In both literal sets and range expressions, quanta and datasets could be
    identified via:

    - UUID literals;
    - combination of task label or dataset type name and data ID (e.g.
      ``makeWarp@{tract=9813, patch=42, visit=12}``);
    - bind aliases for UUIDs, sets of UUIDs, task labels, dataset type names,
      and data IDs.
    - the values of the `QuantumStatus` and `DatasetStatus` enums as keywords,
      representing the set of all quanta or datasets with that status.

    Data IDs keys that are common to all selected nodes can be provided by
    butler defaults or a separate single data ID, which should be accepted by
    any method accepting an expression of this type.

    In queries that span multiple collections, dataset and quantum identifiers
    that use data IDs are resolved in the order they appear in the collection
    sequence, but range expressions can pick up datasets and quanta that are
    shadowed.
    """


class QuantumStatus(enum.Enum):
    """Valid states for a quantum node.

    This is intended only to capture status to the extent necessary to
    represent state transitions when executing or retrying quanta; full status
    information that's useful for diagnostic purposes does not belong here.
    """

    BUILT = enum.auto()
    """Quantum has been predicted but is not known to have been started or
    finished.
    """

    STARTED = enum.auto()
    """Execution has started but is not complete.

    Workspace implementations are not required to ever set this state.
    """

    SUCCEEDED = enum.auto()
    """Execution completed successfully.

    This includes the case where the quantum had no work to do, which can be
    determined by looking at the status of downstream dataset nodes.
    """

    FAILED = enum.auto()
    """Execution was started but raised an exception or otherwise failed.

    When this status is set on a quantum node, the node may have an
    ``exception_type`` attribute with the exception type, a `str`
    ``message`` attribute with the exception message, and a ``traceback``
    attribute with the exception traceback, *if* an exception was caught.
    """


class DatasetStatus(enum.Flag):
    """Valid states for a dataset node in quantum graph."""

    PREDICTED = enum.auto()
    """Dataset has been predicted to exist but is not known to have been
    written or invalidated.
    """

    PRESENT = enum.auto()
    """Dataset has been produced, and the producing quantum was either
    successful or is still running.

    This is all state for all overall-inputs to the workspace that were
    present in the central data repository.
    """

    INVALIDATED = enum.auto()
    """Dataset has been produced but the producing task later failed.
    """


class QuantumGraph(ABC):
    """A directed acyclic graph of datasets and the quanta that produce and
    consume them.

    The attributes here do not necessarily map to how the graph is stored,
    which is workspace- and (for committed graphs) registry-defined.

    This base class needs to live in ``daf_butler`` so it can be used as the
    return type for provenance queries; note that it sees quanta only as
    entities that consume and produce datasets that are identified by a UUID or
    the combination of a task label and a data ID, and it sees tasks as just
    string labels.
    """

    def get_bipartite_xgraph(self) -> nx.MultiDiGraph:
        """Return a directed acyclic graph with UUID keys for both quanta and
        datasets.

        Notes
        -----
        This is a `networkx.MultiDiGraph` to represent the possibility of a
        quantum consuming a single dataset through multiple connections (edge
        keys are connection names).

        Nodes have a ``bipartite`` attribute that is set to ``0`` for datasets
        and ``1`` for quanta.  This can be used by the functions in the
        `networkx.algorithms.bipartite` package.  Nodes representing quanta
        also have a ``label`` attribute, while nodes representing datasets have
        a ``dataset_type_name`` attribute.  Additional information about tasks
        and dataset types can be looked up via the `pipeline_graph` attribute.

        This data structure intentionally does not carry data ID or
        `~lsst.daf.butler.DatasetRef` objects, to avoid loading those (which
        can be expensive) when they are not actually needed.  Use `get_data_id`
        and `get_dataset_ref` to obtain them.
        """
        raise NotImplementedError()

    def get_quantum_xgraph(self) -> nx.DiGraph:
        """Return a directed acyclic graph with UUIDs key for quanta only.

        Nodes have a ``label`` attribute only.  Edges have no attributes.
        """

    def get_data_id(self, node_id: uuid.UUID) -> DataCoordinate:
        """Return the data ID for a quantum or dataset.

        Data IDs are guaranteed to be fully expanded.
        """
        raise NotImplementedError()

    def get_dataset_ref(self, dataset_id: uuid.UUID) -> DatasetRef:
        """Return the `~lsst.daf.butler.DatasetRef` object for a dataset node.

        Data IDs are guaranteed to be fully expanded.
        """
        raise NotImplementedError()

    def get_quanta_for_task(self, label: str) -> Set[uuid.UUID]:
        """Return all quanta for a single task."""
        raise NotImplementedError()

    def find_quantum(self, label: str, data_id: DataId) -> uuid.UUID:
        """Find a single quantum by data ID.

        This method will raise if the result is not unique, which can happen
        in rare cases if the graph spans multiple collections.  `find_quanta`
        is usually less convenient but can be used to handle this case.
        """
        raise NotImplementedError()

    def find_quanta(self, label: str, data_id: DataId) -> Set[uuid.UUID]:
        """Find all quanta with a particular task and data ID."""
        raise NotImplementedError()

    def get_datasets_for_type(self, dataset_type_name: str) -> Set[uuid.UUID]:
        """Return all datasets for a dataset type."""
        raise NotImplementedError()

    def find_dataset(self, dataset_type_name: str, data_id: DataId) -> uuid.UUID:
        """Find a single dataset by data ID.

        This method will raise if the result is not unique, which can happen
        in rare cases if the graph spans multiple collections.  `find_datasets`
        is usually less convenient but can be used to handle this case.
        """
        raise NotImplementedError()

    def find_datasets(self, label: str, data_id: DataId) -> Set[uuid.UUID]:
        """Find all datasets with a particular dataset type and data ID."""
        raise NotImplementedError()

    def get_quantum_status(self, quantum_id: uuid.UUID) -> QuantumStatus:
        """Get the current status or a quantum."""
        return self.get_quantum_history(quantum_id)[-1]

    def get_dataset_status(self, dataset_id: uuid.UUID) -> DatasetStatus:
        """Get the current status for a dataset."""
        return self.get_dataset_history(dataset_id)[-1]

    def get_quantum_history(self, quantum_id: uuid.UUID) -> list[QuantumStatus]:
        """Get the history of this quantum's status.

        Current status is last.
        """
        raise NotImplementedError()

    def get_dataset_history(self, dataset_id: uuid.UUID) -> list[DatasetStatus]:
        """Get the history of this dataset's status.

        Current status is last.
        """
        raise NotImplementedError()

    # TODO: convenience methods for the most common graph-traversal operations.


class WorkspaceButler(LimitedButler):
    """Limited butler that knows about a single workspace.

    Notes
    -----
    This is an intermediate base class that doesn't assume anything about
    PipelineTasks or Pipelines (or even quanta, despite the act that the full
    butler will know about quanta), making it safe to put in ``daf_butler``.
    I'm not sure if there will be subclasses other than those that work with
    PipelineTask - this may just be a way to avoid having to move all of
    ``pipe_base`` into ``daf_butler`` - but maybe SPHEREx or RubinTV's
    best-effort processing would use it.

    Workspace butlers do not connect to the central SQL database by default,
    but they do generally store the central data repository's configuration,
    and may construct a full `Butler` internally when needed. Methods that do
    this are always documented as such (all others are not permitted to connect
    to the database).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of this workspace.

        This will be come the name of a `~lsst.daf.butler.CollectionType.RUN`
        collection when the workspace is committed.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def input_collections(self) -> Sequence[str]:
        """Collections used as inputs to any the processes that write datasets
        to this workspace.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def chain(self) -> str | None:
        """The name of an output `~lsst.daf.butler.CollectionType.CHAINED`
        collection that will be created or updated to reflect this workspace's
        inputs and outputs when it is committed.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_external(self) -> bool:
        """If `True`, the central data repository does not maintain any record
        of this workspace and does not manage it.

        External workspaces *do* typically maintain a link back to the central
        repository (e.g. a URI to the central repository's butler config file),
        but the file artifacts for datasets written to an external workspace do
        not go to locations managed by the central repository datastore, and
        hence typically must be transferred when committed.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def root(self) -> ResourcePath | None:
        """Root directory of the workspace if it is external, or `None` if it
        is internal to the central data repository.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def sharded_by(self) -> DimensionGraph:
        """Dimensions along with this workspace's outputs can be committed.

        By default, workspaces are committed in full in a single operation,
        which corresponds to this being an empty set.  For certain use cases
        (at least prompt processing), we need to run processing in the same
        workspace many times over disjoint sets of data IDs and commit each
        set independently.
        """
        raise NotImplementedError()

    @abstractmethod
    def commit(
        self, shards: Iterable[DataCoordinate] | EllipsisType = ..., transfer: str | None = None
    ) -> None:
        """Register all datasets produced in this workspace with the central
        data repository, and invalidate the workspace.

        Parameters
        ----------
        data_id : `~lsst.daf.butler.DataCoordinate` or mapping, optional
            If provided, only commit certain data IDs (which must correspond to
            the workspace's sharding dimensions), and do not invalidate the
            workspace as a whole.  Datasets and provenance with these data IDs
            are removed from the workspace and added to a ``RUN`` collection in
            the central repository with the same name.
        transfer : `str`, optional
            Transfer method used to ingest datasets into the central data
            repository.  Ignored unless if this is an internal workspace.

        Notes
        -----
        Committing a workspace also transfers provenance information to the
        registry, which may (or may not!) include moving content between
        databases, between databases and files, or consolidating files, even
        when the workspace is internal.

        This method writes to the central data repository's database.
        """

    @abstractmethod
    def get_committed_shards(self) -> Set[DataCoordinate]:
        """Return the shards that have already been committed and removed from
        this workspace, despite originating here.
        """
        raise NotImplementedError()

    @abstractmethod
    def abandon(self) -> None:
        """Delete the workspace and all of its contents.

        Notes
        -----
        This operation invalidates the workspace object.
        """
        raise NotImplementedError()

    def export(
        self, *, root: ResourcePathExpression | None = None, transfer: str | None = None, **kwargs: Any
    ) -> Butler:
        """Create a new independent data repository from the output datasets in
        this workspace.

        Parameters
        ----------
        root : convertible to `lsst.resources.ResourcePath`, optional
            Root for the new data repository.  Must be provided if this is an
            internal data repository.
        transfer : `str` or `None`, optional
            Transfer mode for ingesting file artifacts into the new data
            repository.  Ignored if ``root`` is `None` or the same as the
            workspace's current root.
        **kwargs
            Forwarded to `Butler.makeRepo`.

        Returns
        -------
        butler : `Butler`
            A full butler client for the new data repository.

        Notes
        -----
        This operation invalidates the workspace object.
        """
        raise NotImplementedError()


_W = TypeVar("_W", bound=WorkspaceButler)


class Butler:
    """Full butler, with direct access to central database."""

    _config: ButlerConfig
    dimensions: DimensionUniverse

    def make_workspace(
        self,
        name: str,
        input_collections: Sequence[str],
        factory: Callable[
            [str, Sequence[str], DimensionGraph, str | None, ResourcePath | None, Butler, ButlerConfig],
            WorkspaceButler,
        ],
        *,
        shard_by: Iterable[str] | DimensionGraph | None = None,
        chain: str | None = None,
        path: ResourcePathExpression | None = None,
    ) -> WorkspaceButler:
        """Make a new workspace.

        Parameters
        ----------
        name : `str`
            Name of the workspace.  This can be used to retrieve it later (via
            `get_workspace`) and will be used as the name of the ``RUN``
            collection created when the workspace is committed.
        input_collections : `~collections.abc.Sequence` [ `str` ]
            Collections whose inputs may be used by the workspace.
        factory : `~collections.abc.Callable`
            Callback that is passed all arguments to this method (after
            transforming some defaults) as well as ``self`` and the butler
            configuration to construct the actual workspace.
        shard_by : `~collections.abc.Iterable` [ `str` ] or `DimensionGraph`, \
                optional
            Dimensions for data IDs that can be separately committed from this
            workspace.
        chain : `str`, optional
            Name of a chained collection to create or modify to include the new
            output run and the input collections on commit.
        path : convertible to `lsst.resources.ResourcePath`, optional
            If not `None`, a path to an external directory where the
            workspace's file artifacts should be stored.  This marks the
            workspace as "external", which means that those artifacts must be
            transferred in order to be committed, but it is easier to set up a
            standalone data repository from them instead (see
            `WorkspaceButler.export`).

        Notes
        -----
        We may be able to add some syntactic sugar to make this pattern a
        little simpler for users, but we may not have to if they always use a
        CLI command defined in ``pipe_base`` to do it anyway.  The idea of
        having a callback here is that the central data repository's
        configuration may have options that workspace factories could read when
        a new workspace is constructed, like queues for a BPS workspace.
        """
        match shard_by:
            case None:
                shard_by = self.dimensions.empty
            case DimensionGraph():
                pass
            case _:
                shard_by = self.dimensions.extract(shard_by)
        path = ResourcePath(path) if path is not None else None
        return factory(name, input_collections, shard_by, chain, path, self, self._config)

    def get_workspace(self, name: str) -> WorkspaceButler:
        """Return an existing internal workspace."""
        raise NotImplementedError()

    def query_provenance(
        self,
        collections: Any,
        *,
        where: QuantumGraphExpression = ...,
        bind: Mapping[str, Any] | None = None,
        data_id: DataId | None = None,
        **kwargs: Any,
    ) -> QuantumGraph:
        """Query for provenance as a directed-acyclic graph of datasets and
        'quanta' that produce them.

        Notes
        -----
        This only considers provenance that has been committed to the central
        data repository.

        The implementation is not expected to be optimized for the case of
        querying all (or a very large number) of collections with a highly
        constraining ``where`` expression; provenance graph storage will
        probably be run-by-run rather than indexed by data ID or
        dataset/quantum UUID.  This means it may be much more efficient to
        first query for datasets using data ID expressions in order to
        extract their ``RUN`` collection names before querying for their
        provenance.
        """
        raise NotImplementedError()
