from __future__ import annotations

__all__ = (
    "QuantumGraphExpression",
    "QuantumStatus",
    "DatasetStatus",
    "QuantumGraph",
    "WorkspaceButler",
    "Butler",
)

import enum
import uuid
from abc import abstractmethod, ABC
from collections.abc import Iterable, Mapping, Sequence, Set
import json
from types import EllipsisType
from typing import Any, ClassVar, Protocol, Self, cast, final

from pydantic import BaseModel

import networkx as nx  # type: ignore[import]
from lsst.daf.butler import (
    Config,
    DataCoordinate,
    DataId,
    DatasetRef,
    DimensionGraph,
    DimensionUniverse,
    LimitedButler,
    Registry,
)
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import get_full_type_name
from lsst.utils.doImport import doImportType


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

    Notes
    -----
    Datasets are included in a quantum graph if and only if a producing or
    consuming quantum is included; there are never isolated dataset nodes with
    no edges in a graph, and any quantum in the graph can be assumed to have
    all of its nodes.

    The attributes here do not necessarily map to how the graph is stored,
    which is workspace- and (for committed graphs) registry-defined.

    This base class needs to live in ``daf_butler`` so it can be used as the
    return type for provenance queries; note that it sees quanta only as
    entities that consume and produce datasets that are identified by a UUID or
    the combination of a task label and a data ID, and it sees tasks as just
    string labels.
    """

    @property
    @abstractmethod
    def universe(self) -> DimensionUniverse:
        """Definitions for any dimensions used by this graph."""
        raise NotImplementedError()

    @abstractmethod
    def get_bipartite_xgraph(self, *, annotate: bool = False) -> nx.MultiDiGraph:
        """Return a directed acyclic graph with UUID keys for both quanta and
        datasets.

        Parameters
        ----------
        annotate : `bool`, optional
            If `True`, add expanded `lsst.daf.butler.DataCoordinate` objects
            and `lsst.daf.butler.DatasetRef` to nodes (the latter only for
            dataset nodes) as ``data_id`` and ``ref`` attributes.

        Returns
        -------
        graph : `networkx.MultiDiGraph`
            NetworkX directed graph with quantum and dataset nodes, where edges
            only connect quanta to datasets and datasets to quanta.  This is a
            `networkx.MultiDiGraph` to represent the possibility of a quantum
            consuming a single dataset through multiple connections (edge keys
            are connection names).

        Notes
        -----
        Nodes have a ``bipartite`` attribute that is set to ``0`` for datasets
        and ``1`` for quanta.  This can be used by the functions in the
        `networkx.algorithms.bipartite` package.  Nodes representing quanta
        also have a ``label`` attribute, while nodes representing datasets have
        a ``dataset_type_name`` attribute.  All nodes have a ``run`` attribute
        with the name of the run collection or workspace they belong to.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_quantum_xgraph(self, annotate: bool = False) -> nx.DiGraph:
        """Return a directed acyclic graph with UUIDs key for quanta only.

        Parameters
        ----------
        annotate : `bool`, optional
            If `True`, add expanded `lsst.daf.butler.DataCoordinate` objects
            to nodes as ``data_id`` attributes.

        Returns
        -------
        graph : `networkx.MultiDiGraph`
            NetworkX directed graph with quantum nodes only, with edges
            representing one or more datasets produced by one quantum and
            consumed by the other.

        Nodes
        -----
            Unless ``annotate=True``, nodes have ``label`` and ``run``
            attributes only.  Edges have no attributes.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_data_id(self, node_id: uuid.UUID) -> DataCoordinate:
        """Return the data ID for a quantum or dataset.

        Data IDs are guaranteed to be fully expanded.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_dataset_ref(self, dataset_id: uuid.UUID) -> DatasetRef:
        """Return the `~lsst.daf.butler.DatasetRef` object for a dataset node.

        Data IDs are guaranteed to be fully expanded.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_quanta_for_task(self, label: str) -> Set[uuid.UUID]:
        """Return all quanta for a single task."""
        raise NotImplementedError()

    @abstractmethod
    def find_quantum(self, label: str, data_id: DataId) -> uuid.UUID:
        """Find a single quantum by data ID.

        This method will raise if the result is not unique, which can happen
        in rare cases if the graph spans multiple collections.  `find_quanta`
        is usually less convenient but can be used to handle this case.
        """
        raise NotImplementedError()

    @abstractmethod
    def find_quanta(self, label: str, data_id: DataId) -> Set[uuid.UUID]:
        """Find all quanta with a particular task and data ID."""
        raise NotImplementedError()

    @abstractmethod
    def get_datasets_for_type(self, dataset_type_name: str) -> Set[uuid.UUID]:
        """Return all datasets for a dataset type."""
        raise NotImplementedError()

    @abstractmethod
    def find_dataset(self, dataset_type_name: str, data_id: DataId) -> uuid.UUID:
        """Find a single dataset by data ID.

        This method will raise if the result is not unique, which can happen
        in rare cases if the graph spans multiple collections.  `find_datasets`
        is usually less convenient but can be used to handle this case.
        """
        raise NotImplementedError()

    @abstractmethod
    def find_datasets(self, label: str, data_id: DataId) -> Set[uuid.UUID]:
        """Find all datasets with a particular dataset type and data ID."""
        raise NotImplementedError()

    @abstractmethod
    def get_quantum_status(self, quantum_id: uuid.UUID) -> QuantumStatus:
        """Get the current status or a quantum."""
        return self.get_quantum_history(quantum_id)[-1]

    @abstractmethod
    def get_dataset_status(self, dataset_id: uuid.UUID) -> DatasetStatus:
        """Get the current status for a dataset."""
        return self.get_dataset_history(dataset_id)[-1]

    @abstractmethod
    def get_quantum_history(self, quantum_id: uuid.UUID) -> list[QuantumStatus]:
        """Get the history of this quantum's status.

        Current status is last.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_dataset_history(self, dataset_id: uuid.UUID) -> list[DatasetStatus]:
        """Get the history of this dataset's status.

        Current status is last.
        """
        raise NotImplementedError()

    @abstractmethod
    def filtered(
        self,
        where: QuantumGraphExpression = ...,
        bind: Mapping[str, Any] | None = None,
        data_id: DataId | None = None,
        **kwargs: Any,
    ) -> QuantumGraph:
        """Filter the quanta and datasets in this graph according to an
        expression.
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

    def __init__(
        self,
        name: str,
        input_collections: Sequence[str],
        chain: str | None,
        parent_config: ButlerConfig,
        parent_write_butler: Butler | None,
        parent_read_butler: Butler | None,
    ):
        self._name = name
        self._input_collections = input_collections
        self._chain = chain
        self._parent_config = parent_config
        self._parent_read_butler = parent_read_butler
        self._parent_write_butler = parent_write_butler

    @classmethod
    def make_external(cls, root: ResourcePathExpression) -> Self:
        """Construct a workspace butler of the appropriate derived type from
        an external workspace path.
        """
        root = ResourcePath(root, forceDirectory=True)
        with root.join(WorkspaceConfig.FILENAME).open("r") as stream:
            config_data = json.load(stream)
        config_cls = doImportType(config_data["type_name"])
        config: WorkspaceConfig = config_cls(**config_data)
        butler = config.make_butler()
        if not isinstance(butler, cls):
            raise TypeError(
                f"Unexpected workspace butler class; config file specifies {type(butler).__name__}, but "
                f"make_external was called on {cls.__name__}."
            )
        return butler

    @property
    def name(self) -> str:
        """Name of this workspace.

        This will be come the name of a `~lsst.daf.butler.CollectionType.RUN`
        collection when the workspace is committed.
        """
        return self._name

    @property
    def input_collections(self) -> Sequence[str]:
        """Collections used as inputs to any the processes that write datasets
        to this workspace.
        """
        return self._input_collections

    @property
    def chain(self) -> str | None:
        """The name of an output `~lsst.daf.butler.CollectionType.CHAINED`
        collection that will be created or updated to reflect this workspace's
        inputs and outputs when it is committed.
        """
        return self._chain

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

    @final
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
        if shards is not ...:
            raise NotImplementedError("TODO: check and then update committed shards.")
        parent = self._get_parent_write_butler()
        self._transfer_content(parent, shards=shards, transfer=transfer)
        if shards is ...:
            self.abandon()

    @abstractmethod
    def get_committed_shards(self) -> Set[DataCoordinate]:
        """Return the shards that have already been committed and removed from
        this workspace, despite originating here.
        """
        raise NotImplementedError()

    @final
    def abandon(self) -> None:
        """Delete the workspace and all of its contents.

        Notes
        -----
        This operation invalidates the workspace object.
        """
        try:
            self._remove_remaining_content()
        except BaseException as err:
            raise CorruptedWorkspaceError(
                f"Workspace {self.name} left in an inconsistent state by 'abandon'."
            ) from err
        self._get_parent_write_butler()._remove_workspace_record(self.name)

    @final
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
        if root is None:
            root = self.root
            if root is None:
                raise ValueError("Cannot export an internal workspace without a root.")
        else:
            root = ResourcePath(root, forceDirectory=True)
        if root != self.root and transfer is None:
            raise ValueError(
                "A transfer mode is required to export a repository from a workspace with a different root."
            )
        full_config = ButlerConfig(Butler.makeRepo(root, **kwargs))
        full_butler = Butler(full_config, writeable=True)
        self._insert_dimension_records(full_butler)
        self._transfer_content(full_butler, shards=..., transfer=transfer)
        self.abandon()
        return full_butler

    @final
    def _get_parent_read_butler(self) -> Butler:
        """Return a read-only full butler for the central repository."""
        if self._parent_read_butler is None:
            self._parent_read_butler = Butler(self._parent_config, collections=self.input_collections)
        return self._parent_read_butler

    @final
    def _get_parent_write_butler(self) -> Butler:
        """Return a read-write full butler for the central repository."""
        if self._parent_write_butler is None:
            self._parent_write_butler = Butler(
                self._parent_config, writeable=True, collections=self.input_collections
            )
        return self._parent_write_butler

    @abstractmethod
    def _transfer_content(
        self,
        parent: Butler,
        shards: Iterable[DataCoordinate] | EllipsisType = ...,
        transfer: str | None = None,
    ) -> None:
        """Transfer workspace datasets and provenance to a full butler.

        This should remove this state from the workspace's own record-keeping
        as it goes to keep it from being deleted by `_remove_persistent_state`
        when that is called later, since the workspace's file artifacts will
        frequently be ingested into the central data repository with
        ``transfer=None`` and we don't want those to be deleted along with the
        workspace.
        """
        raise NotImplementedError()

    @abstractmethod
    def _remove_remaining_content(self) -> None:
        """Delete the persistent state of the workspace."""
        raise NotImplementedError()

    @abstractmethod
    def _insert_dimension_records(self, parent: Butler) -> None:
        """Insert all dimension records needed by the data IDs of this
        workspace's datasets and provenance into the given butler.
        """
        raise NotImplementedError()


class WorkspaceFactory(Protocol):
    """An interface for callables that construct new workspaces.

    Implementations of this protocol may be regular methods or functions, but
    we expect them to frequently be full types so instance state can be used to
    hold workspace-specific initialization state.
    """

    def __call__(
        self,
        name: str,
        input_collections: Sequence[str],
        shard_by: DimensionGraph,
        chain: str | None,
        path: ResourcePath | None,
        parent: Butler,
        parent_config: ButlerConfig,
    ) -> tuple[WorkspaceButler, WorkspaceConfig]:
        ...


class HandledWorkspaceFactoryError(RuntimeError):
    """Exception raised by WorkspaceFactory when it has encountered an error
    but no persistent state has been left behind.

    This error should always be chained to the originating exception.
    """


class CorruptedWorkspaceError(RuntimeError):
    """Exception raised when a workspace is known to be in an inconsistent
    state and requires administrative assistance to be removed.
    """


class WorkspaceConfig(BaseModel, ABC):
    """A configuration object that can construct a `WorkspaceButler` client
    instance for an existing workspace.
    """

    FILENAME: ClassVar[str] = "butler-workspace.json"
    """Filename used for all workspace butler configuration files."""

    type_name: str
    """Fully-qualified Python type for this `WorkspaceConfig` subclass.

    This is populated automatically by the constructor and stored in this
    attribute to make sure it is automatically saved to disk.
    """

    parent_config: dict[str, Any]
    """Parent configuration, as a nested dictionary."""

    name: str
    """Name of the workspace

    This will become the RUN collection name on commit.
    """

    input_collections: list[str]
    """Sequence of collections to search for inputs.
    """

    chain: str | None
    """CHAINED collection to create from input collections and the output
    RUN collection on commit.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(type_name=get_full_type_name(self), **kwargs)

    @abstractmethod
    def make_butler(
        self,
        parent_config: ButlerConfig | None = None,
        parent_read_butler: Butler | None = None,
        parent_write_butler: Butler | None = None,
    ) -> WorkspaceButler:
        """Construct a `WorkspaceButler` instance from this configuration."""
        raise NotImplementedError()


class ButlerConfig:
    """Full butler configuration object."""

    def __init__(self, arg: Any):
        ...

    def __getitem__(self, key: str) -> Any:
        ...

    def get_workspace_config_uri(self, name: str) -> ResourcePath:
        """Return the URI for an internal workspace config file."""
        config_root = ResourcePath(self[".workspaces.config_template"].format(name=name), forceDirectory=True)
        return config_root.join(WorkspaceConfig.FILENAME)

    def make_workspace_butler(
        self,
        name: str,
        parent_read_butler: Butler | None = None,
        parent_write_butler: Butler | None = None,
    ) -> WorkspaceButler:
        """Make a butler client for an existing internal workspace."""
        config_uri = self.get_workspace_config_uri(name)
        with config_uri.open("r") as stream:
            config_data = json.load(stream)
        config_cls = doImportType(config_data["type_name"])
        config: WorkspaceConfig = config_cls(**config_data)
        return config.make_butler(
            self, parent_read_butler=parent_read_butler, parent_write_butler=parent_write_butler
        )


class Butler:
    """Full butler, with direct access to central database."""

    _config: ButlerConfig
    _registry: Registry
    dimensions: DimensionUniverse

    def __init__(self, config: ButlerConfig, *, collections: Sequence[str] = (), writeable: bool = False):
        raise NotImplementedError("Just a stub to satisfy MyPy on the prototype.")

    @classmethod
    def makeRepo(cls, root: ResourcePathExpression, **kwargs: Any) -> Config:
        raise NotImplementedError("Just a stub to satisfy MyPy on the prototype.")

    @property
    def is_writeable(self) -> bool:
        return self._registry.isWriteable()

    def make_workspace(
        self,
        name: str,
        input_collections: Sequence[str],
        factory: WorkspaceFactory,
        *,
        shard_by: Iterable[str] | DimensionGraph | None = None,
        chain: str | None = None,
        root: ResourcePathExpression | None = None,
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
        root : convertible to `lsst.resources.ResourcePath`, optional
            If not `None`, a path to an external directory where the
            workspace's file artifacts should be stored.  This marks the
            workspace as "external", which means that those artifacts must be
            transferred in order to be committed, but it is easier to set up a
            standalone data repository from them instead (see
            `WorkspaceButler.export`).

        Returns
        -------
        workspace : `WorkspaceButler`
            Butler client for the new workspace.

        Notes
        -----
        We may be able to add some syntactic sugar to make this pattern a
        little simpler for users, but we may not have to if they always use a
        CLI command defined in ``pipe_base`` to do it anyway.  The idea of
        having a callback here is that the central data repository's
        configuration may have options that workspace factories could read when
        a new workspace is constructed, like queues for a BPS workspace.
        """
        # Standardize arguments.
        match shard_by:
            case None:
                shard_by = self.dimensions.empty
            case DimensionGraph():  # type: ignore[misc]
                pass
            case _:
                shard_by = self.dimensions.extract(shard_by)
        if root is None:
            # This is an internal workspace.
            config_uri = self._config.get_workspace_config_uri(name)
            # For internal workspaces, we insert a row identifying the
            # workspace into a database table before it is actually created,
            # and fail if such a row already exists.  This is effectively a
            # per-name concurrency lock on the creation of workspaces.
            self._insert_workspace_record(name)
        else:
            # This is an external workspace.
            root = ResourcePath(root, forceDirectory=True)
            config_uri = root.join(WorkspaceConfig.FILENAME)
        # Delegate to the factory object to do most of the work.  This writes
        # persistent state (e.g. to files or a database).
        try:
            workspace_butler, workspace_config = factory(
                name, input_collections, shard_by, chain, root, self, self._config
            )
        except HandledWorkspaceFactoryError as err:
            # An error occurred, but the factory cleaned up its own mess.  We
            # can remove the record of the workspace from the database and
            # just re-raise the original exception.
            self._remove_workspace_record(name)
            raise cast(BaseException, err.__cause__)
        except BaseException as err:
            # An error occurred and the factory cannot guarantee anything about
            # the persistent state.  Make it clear that administrative action
            # is needed.
            #
            # Note that this state is recognizable for internal workspaces from
            # the existence of a central database row for the workspace and the
            # absence of a config file, and that the database row needs to be
            # deleted for an administrator to mark it as cleaned up.  For
            # external workspaces we expect the user to just 'rm -rf' (or
            # equivalent) the workspace directory.
            raise CorruptedWorkspaceError(
                f"New workspace {name} with root {root} was corrupted during construction."
            ) from err
        try:
            # Save a configuration file for the workspace to allow the
            # WorkspaceButler to be reconstructed without the full Butler in
            # the future.
            with config_uri.open("w") as stream:
                stream.write(workspace_config.json())
        except BaseException:
            # If we fail here, try to clean up.
            try:
                workspace_butler._remove_remaining_content()
            except BaseException as err:
                # Couldn't clean up.
                raise CorruptedWorkspaceError(
                    f"New workspace {name} with root {root} was corrupted after to write {config_uri}."
                ) from err
            # Successfully cleaned up workspace persistent state, try to remove
            # from database as well if this is an internal workspace.
            if root is None:
                self._remove_workspace_record(name)
            raise
        return workspace_butler

    def _insert_workspace_record(self, name: str) -> None:
        """Insert a row into the central database to record the existence of
        an internal workspace.
        """
        raise NotImplementedError("TODO: insert database row.")

    def _remove_workspace_record(self, name: str) -> None:
        """Remove the row in the central database that records the existence of
        an internal workspace, after deleting the workspace config file (if it
        exists).
        """
        config_uri = self._config.get_workspace_config_uri(name)
        if config_uri.exists():
            config_uri.remove()
        raise NotImplementedError("TODO: remove database row.")

    def get_workspace(self, name: str) -> WorkspaceButler:
        """Return an existing internal workspace butler.

        Parameters
        ----------
        name : `str`
            Name used to construct the workspace.

        Returns
        -------
        workspace : `WorkspaceButler`
            Butler client for the workspace.
        """
        return self._config.make_workspace_butler(
            name,
            parent_write_butler=self if self.is_writeable else None,
            parent_read_butler=self if not self.is_writeable else None,
        )

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
        extract their ``RUN`` collection names, before querying for their
        provenance in a second pass.
        """
        raise NotImplementedError()
