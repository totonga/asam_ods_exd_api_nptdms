"""Registry for external data file handlers."""

from typing import Callable

from .file_interface import ExternalDataFileInterface  # pylint: disable=import-error


class FileHandlerRegistry:
    """Registry for managing external data file handlers.

    This registry allows registering different file handler implementations
    and creating instances based on file type or extension.
    """

    _handlers: dict[str, Callable[[str, str], ExternalDataFileInterface]] = {}

    @classmethod
    def register(cls, file_type: str, factory: Callable[[str, str], ExternalDataFileInterface]) -> None:
        """Register a file handler factory for a specific file type.

        Args:
            file_type: File type identifier (e.g., 'tdms', 'hdf5')
            factory: Callable that creates ExternalDataFileInterface instances
        """
        cls._handlers[file_type] = factory

    @classmethod
    def create(cls, file_type: str, file_path: str, parameters: str) -> ExternalDataFileInterface:
        """Create an instance of the registered handler for the given file type.

        Args:
            file_type: File type identifier
            file_path: Path to the external data file
            parameters: Optional parameters for file handling

        Returns:
            ExternalDataFileInterface implementation

        Raises:
            ValueError: If file type is not registered
        """
        if not cls._handlers:
            raise ValueError("No file handlers registered")
        factory = cls._handlers.get(file_type) or next(
            iter(cls._handlers.values()))
        return factory(file_path, parameters)

    @classmethod
    def get_file_type(cls, file_path: str) -> str:
        """Determine file type from file extension.

        Args:
            file_path: Path to the file

        Returns:
            File type based on extension
        """
        extension = file_path.split('.')[-1].upper()
        return extension

    @classmethod
    def create_from_path(cls, file_path: str, parameters: str) -> ExternalDataFileInterface:
        """Create an instance by automatically detecting file type from path.

        Args:
            file_path: Path to the external data file
            parameters: Optional parameters for file handling

        Returns:
            ExternalDataFileInterface implementation

        Raises:
            ValueError: If file type cannot be detected or is not registered
        """
        file_type = cls.get_file_type(file_path)
        return cls.create(file_type, file_path, parameters)
