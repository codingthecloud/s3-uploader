import logging
import os
from pathlib import Path

from s3uploader_core import S3Uploader
from s3uploader_core import S3UploaderSettings

try:
    from PySide6.QtCore import QObject
    from PySide6.QtCore import QDir
    from PySide6.QtCore import QModelIndex
    from PySide6.QtCore import QThread
    from PySide6.QtCore import Qt
    from PySide6.QtCore import Signal
    from PySide6.QtGui import QPixmap
    from PySide6.QtWidgets import QApplication
    from PySide6.QtWidgets import QFileSystemModel
    from PySide6.QtWidgets import QFormLayout
    from PySide6.QtWidgets import QGroupBox
    from PySide6.QtWidgets import QHBoxLayout
    from PySide6.QtWidgets import QInputDialog
    from PySide6.QtWidgets import QLabel
    from PySide6.QtWidgets import QListWidget
    from PySide6.QtWidgets import QListWidgetItem
    from PySide6.QtWidgets import QLineEdit
    from PySide6.QtWidgets import QMainWindow
    from PySide6.QtWidgets import QMessageBox
    from PySide6.QtWidgets import QPushButton
    from PySide6.QtWidgets import QProgressBar
    from PySide6.QtWidgets import QSplitter
    from PySide6.QtWidgets import QTabWidget
    from PySide6.QtWidgets import QTreeView
    from PySide6.QtWidgets import QTreeWidget
    from PySide6.QtWidgets import QTreeWidgetItem
    from PySide6.QtWidgets import QTextEdit
    from PySide6.QtWidgets import QVBoxLayout
    from PySide6.QtWidgets import QWidget
    from PySide6.QtWidgets import QComboBox
    from PySide6.QtWidgets import QFileDialog
except ImportError as exc:  # pragma: no cover - runtime dependency
    QApplication = None
    QT_IMPORT_ERROR = exc
else:
    QT_IMPORT_ERROR = None


class UploadWorker(QObject):
    progress = Signal(str, dict)
    finished = Signal(dict)
    failed = Signal(str)

    def __init__(self, settings, paths, bucket_name, prefix):
        super().__init__()
        self.settings = settings
        self.paths = paths
        self.bucket_name = bucket_name
        self.prefix = prefix

    def run(self):
        try:
            uploader = S3Uploader(self.settings)
            result = uploader.upload_paths_to_s3(
                paths=self.paths,
                bucket_name=self.bucket_name,
                s3_prefix=self.prefix,
                progress_callback=self._emit_progress
            )
        except Exception as exc:  # pragma: no cover - UI runtime path
            self.failed.emit(str(exc))
            return
        self.finished.emit(result)

    def _emit_progress(self, event_type, payload):
        self.progress.emit(event_type, payload)


class S3UploaderWindow(QMainWindow):
    IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp"}
    AWS_REGIONS = [
        "af-south-1",
        "ap-east-1",
        "ap-east-2",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-northeast-3",
        "ap-south-1",
        "ap-south-2",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-southeast-3",
        "ap-southeast-4",
        "ap-southeast-5",
        "ap-southeast-6",
        "ap-southeast-7",
        "ca-central-1",
        "ca-west-1",
        "eu-central-1",
        "eu-central-2",
        "eu-north-1",
        "eu-south-1",
        "eu-south-2",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "il-central-1",
        "me-central-1",
        "me-south-1",
        "mx-central-1",
        "sa-east-1",
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
    ]

    def __init__(self):
        super().__init__()
        self.setWindowTitle("S3 Uploader")
        self.resize(1500, 900)
        self.s3_service = None
        self.current_prefix = ""
        self.upload_thread = None
        self.upload_worker = None
        self.queued_paths = []
        self.preview_image_path = None
        self._build_ui()
        self._connect_signals()
        self._set_default_local_root()

    def _build_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        root_layout = QVBoxLayout(central_widget)

        settings_layout = QHBoxLayout()
        root_layout.addLayout(settings_layout)

        aws_group = QGroupBox("AWS Configuration")
        aws_group_layout = QVBoxLayout(aws_group)
        credentials_layout = QFormLayout()
        self.access_key_input = QLineEdit()
        self.access_key_input.setEchoMode(QLineEdit.Password)
        self.secret_key_input = QLineEdit()
        self.secret_key_input.setEchoMode(QLineEdit.Password)
        self.session_token_input = QLineEdit()
        self.session_token_input.setEchoMode(QLineEdit.Password)
        self.region_input = QComboBox()
        self.region_input.setEditable(True)
        self.region_input.addItems(self.AWS_REGIONS)
        self.region_input.setCurrentText("eu-west-1")
        self.endpoint_input = QLineEdit()
        self.endpoint_input.setPlaceholderText("Optional for AWS; leave blank")
        self.profile_input = QLineEdit()
        credentials_layout.addRow("Access Key", self.access_key_input)
        credentials_layout.addRow("Secret Key", self.secret_key_input)
        credentials_layout.addRow("Session Token", self.session_token_input)
        credentials_layout.addRow("Region", self.region_input)
        credentials_layout.addRow("Endpoint URL", self.endpoint_input)
        credentials_layout.addRow("Profile", self.profile_input)
        aws_group_layout.addLayout(credentials_layout)
        aws_actions_layout = QHBoxLayout()
        self.connect_button = QPushButton("Connect to AWS")
        self.connect_button.setMinimumHeight(42)
        self.connect_button.setStyleSheet("font-weight: bold;")
        aws_actions_layout.addWidget(self.connect_button)
        aws_group_layout.addLayout(aws_actions_layout)
        settings_layout.addWidget(aws_group, 2)

        transfer_group = QGroupBox("Transfer Settings")
        transfer_group_layout = QVBoxLayout(transfer_group)
        bucket_layout = QFormLayout()
        self.bucket_combo = QComboBox()
        self.bucket_combo.setEditable(True)
        self.prefix_input = QLineEdit()
        self.chunk_size_input = QComboBox()
        self.chunk_size_input.setEditable(True)
        self.chunk_size_input.addItems(["5", "8", "16", "32", "64", "128", "256"])
        self.chunk_size_input.setCurrentText("64")
        self.refresh_buckets_button = QPushButton("Refresh Buckets")
        self.create_bucket_button = QPushButton("Create Bucket")
        self.refresh_s3_button = QPushButton("Refresh S3")
        bucket_layout.addRow("Bucket", self.bucket_combo)
        bucket_layout.addRow("Target Prefix", self.prefix_input)
        bucket_layout.addRow("Chunk Size MB", self.chunk_size_input)
        transfer_group_layout.addLayout(bucket_layout)
        transfer_actions_layout = QHBoxLayout()
        transfer_actions_layout.addWidget(self.refresh_buckets_button)
        transfer_actions_layout.addWidget(self.create_bucket_button)
        transfer_actions_layout.addWidget(self.refresh_s3_button)
        transfer_group_layout.addLayout(transfer_actions_layout)
        settings_layout.addWidget(transfer_group, 3)

        splitter = QSplitter(Qt.Horizontal)
        root_layout.addWidget(splitter, 1)

        local_panel = QWidget()
        local_layout = QHBoxLayout(local_panel)

        local_browser_panel = QWidget()
        local_browser_layout = QVBoxLayout(local_browser_panel)
        local_header_layout = QHBoxLayout()
        local_header_layout.addWidget(QLabel("Local Files"))
        self.local_home_button = QPushButton("⌂")
        self.local_home_button.setToolTip("Go to home folder")
        self.local_home_button.setMaximumWidth(44)
        self.local_home_button.setMinimumHeight(28)
        local_header_layout.addWidget(self.local_home_button)
        self.local_desktop_button = QPushButton("🖥")
        self.local_desktop_button.setToolTip("Go to desktop folder")
        self.local_desktop_button.setMaximumWidth(44)
        self.local_desktop_button.setMinimumHeight(28)
        local_header_layout.addWidget(self.local_desktop_button)
        local_header_layout.addStretch(1)
        local_browser_layout.addLayout(local_header_layout)
        self.local_model = QFileSystemModel(self)
        self.local_model.setRootPath(QDir.rootPath())
        self.local_model.setFilter(QDir.AllDirs | QDir.Files | QDir.NoDotAndDotDot)
        self.local_tree = QTreeView()
        self.local_tree.setModel(self.local_model)
        self.local_tree.setSelectionMode(QTreeView.ExtendedSelection)
        self.local_tree.setSortingEnabled(True)
        for column in range(1, 4):
            self.local_tree.hideColumn(column)
        local_browser_layout.addWidget(self.local_tree, 1)

        queue_actions_panel = QWidget()
        queue_actions_layout = QVBoxLayout(queue_actions_panel)
        queue_actions_layout.addStretch(1)
        self.add_to_queue_button = QPushButton("+")
        self.add_to_queue_button.setToolTip("Add selected local files or folders to the queue")
        self.remove_from_queue_button = QPushButton("-")
        self.remove_from_queue_button.setToolTip("Remove selected items from the queue")
        self.clear_queue_button = QPushButton("×")
        self.clear_queue_button.setToolTip("Clear the transfer queue")
        for button in (self.add_to_queue_button, self.remove_from_queue_button, self.clear_queue_button):
            button.setMinimumWidth(56)
            button.setMaximumWidth(64)
            button.setMinimumHeight(42)
            button.setStyleSheet("font-size: 18px; font-weight: bold;")
        queue_actions_layout.addWidget(self.add_to_queue_button)
        queue_actions_layout.addWidget(self.remove_from_queue_button)
        queue_actions_layout.addWidget(self.clear_queue_button)
        queue_actions_layout.addStretch(1)

        queue_panel = QWidget()
        queue_layout = QVBoxLayout(queue_panel)
        queue_layout.addWidget(QLabel("Transfer Queue"))
        self.queue_list = QListWidget()
        self.queue_list.setSelectionMode(QListWidget.ExtendedSelection)
        queue_layout.addWidget(self.queue_list, 1)
        self.upload_button = QPushButton("Start Transfer")
        self.upload_button.setMinimumHeight(56)
        self.upload_button.setStyleSheet("font-weight: bold;")
        queue_layout.addWidget(self.upload_button)

        local_layout.addWidget(local_browser_panel, 5)
        local_layout.addWidget(queue_actions_panel, 1)
        local_layout.addWidget(queue_panel, 3)

        splitter.addWidget(local_panel)

        s3_panel = QWidget()
        s3_layout = QVBoxLayout(s3_panel)
        s3_layout.addWidget(QLabel("S3"))
        self.s3_tabs = QTabWidget()
        s3_layout.addWidget(self.s3_tabs, 1)

        browser_tab = QWidget()
        browser_layout = QVBoxLayout(browser_tab)
        current_path_layout = QHBoxLayout()
        self.s3_up_button = QPushButton("↑")
        self.s3_up_button.setToolTip("Up one level")
        self.s3_up_button.setMaximumWidth(48)
        self.s3_up_button.setMinimumHeight(32)
        self.s3_up_button.setStyleSheet("font-size: 18px; font-weight: bold;")
        current_path_layout.addWidget(self.s3_up_button)
        self.current_path_label = QLabel("Current prefix: /")
        current_path_layout.addWidget(self.current_path_label)
        current_path_layout.addStretch(1)
        browser_layout.addLayout(current_path_layout)
        s3_actions_layout = QHBoxLayout()
        self.create_folder_button = QPushButton("Create Folder")
        self.download_object_button = QPushButton("Download Object")
        self.delete_object_button = QPushButton("Delete Object")
        self.delete_folder_button = QPushButton("Delete Folder")
        self.download_object_button.setEnabled(False)
        self.delete_object_button.setEnabled(False)
        self.delete_folder_button.setEnabled(False)
        s3_actions_layout.addWidget(self.create_folder_button)
        s3_actions_layout.addWidget(self.download_object_button)
        s3_actions_layout.addWidget(self.delete_object_button)
        s3_actions_layout.addWidget(self.delete_folder_button)
        browser_layout.addLayout(s3_actions_layout)
        self.s3_tree = QTreeWidget()
        self.s3_tree.setColumnCount(3)
        self.s3_tree.setHeaderLabels(["Name", "Type", "Size"])
        browser_layout.addWidget(self.s3_tree, 1)
        self.s3_tabs.addTab(browser_tab, "S3 Browser")

        lifecycle_tab = QWidget()
        lifecycle_layout = QVBoxLayout(lifecycle_tab)
        self.lifecycle_output = QTextEdit()
        self.lifecycle_output.setReadOnly(True)
        self.lifecycle_output.setPlaceholderText("Select a bucket to view lifecycle policies.")
        lifecycle_layout.addWidget(self.lifecycle_output, 1)
        self.s3_tabs.addTab(lifecycle_tab, "Lifecycle Policies")

        splitter.addWidget(s3_panel)
        splitter.setSizes([560, 840])

        action_layout = QHBoxLayout()
        root_layout.addLayout(action_layout)
        self.status_label = QLabel("Status: Ready to transfer")
        self.status_label.setStyleSheet("font-style: italic; color: #555;")
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(1)
        self.progress_bar.setValue(0)
        action_layout.addWidget(self.status_label, 1)
        action_layout.addWidget(self.progress_bar, 1)

        bottom_splitter = QSplitter(Qt.Horizontal)
        root_layout.addWidget(bottom_splitter, 1)

        preview_panel = QWidget()
        preview_layout = QVBoxLayout(preview_panel)
        preview_layout.addWidget(QLabel("Image Preview"))
        self.preview_caption = QLabel("Select a local image to preview.")
        self.preview_caption.setWordWrap(True)
        preview_layout.addWidget(self.preview_caption)
        self.preview_image = QLabel("No image selected")
        self.preview_image.setAlignment(Qt.AlignCenter)
        self.preview_image.setMinimumHeight(220)
        self.preview_image.setStyleSheet("border: 1px solid #555; color: #777;")
        preview_layout.addWidget(self.preview_image, 1)
        bottom_splitter.addWidget(preview_panel)

        logs_panel = QWidget()
        logs_layout = QVBoxLayout(logs_panel)
        logs_layout.addWidget(QLabel("Logs"))
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        logs_layout.addWidget(self.log_output, 1)
        bottom_splitter.addWidget(logs_panel)
        bottom_splitter.setSizes([320, 880])

    def _connect_signals(self):
        self.connect_button.clicked.connect(self.connect_to_s3)
        self.refresh_buckets_button.clicked.connect(self.refresh_buckets)
        self.create_bucket_button.clicked.connect(self.create_bucket)
        self.bucket_combo.currentTextChanged.connect(self._on_bucket_changed)
        self.refresh_s3_button.clicked.connect(self.refresh_s3_browser)
        self.create_folder_button.clicked.connect(self.create_folder)
        self.download_object_button.clicked.connect(self.download_selected_object)
        self.delete_object_button.clicked.connect(self.delete_selected_object)
        self.delete_folder_button.clicked.connect(self.delete_selected_folder)
        self.s3_up_button.clicked.connect(self.navigate_up)
        self.s3_tree.itemDoubleClicked.connect(self.open_s3_item)
        self.s3_tree.itemSelectionChanged.connect(self.update_s3_action_buttons)
        self.local_home_button.clicked.connect(lambda: self._set_local_root(QDir.homePath()))
        self.local_desktop_button.clicked.connect(self.go_to_desktop_folder)
        self.add_to_queue_button.clicked.connect(self.add_selected_to_queue)
        self.remove_from_queue_button.clicked.connect(self.remove_selected_from_queue)
        self.clear_queue_button.clicked.connect(self.clear_queue)
        self.upload_button.clicked.connect(self.upload_selected)
        self.local_tree.selectionModel().selectionChanged.connect(self.update_local_preview)

    def _set_local_root(self, path):
        root_index = self.local_model.index(path)
        if root_index.isValid():
            self.local_tree.setRootIndex(root_index)

    def _set_default_local_root(self):
        if os.name == "nt":
            self.local_tree.setRootIndex(QModelIndex())
            return
        self._set_local_root(QDir.homePath())

    def go_to_desktop_folder(self):
        desktop_path = str(Path.home() / "Desktop")
        if Path(desktop_path).exists():
            self._set_local_root(desktop_path)
            return
        self.show_error("Desktop folder was not found.")

    def resizeEvent(self, event):  # pragma: no cover - UI runtime path
        super().resizeEvent(event)
        self._render_preview_pixmap()

    def _settings_from_form(self, bucket_name=None):
        chunk_size = int(self.chunk_size_input.currentText().strip() or S3UploaderSettings.DEFAULT_CHUNK_SIZE)
        return S3UploaderSettings(
            bucket_name=bucket_name,
            s3_prefix=self.prefix_input.text().strip(),
            chunk_size_mb=chunk_size,
            endpoint_url=self.endpoint_input.text().strip() or None,
            region_name=self.region_input.currentText().strip() or None,
            aws_access_key_id=self.access_key_input.text().strip() or None,
            aws_secret_access_key=self.secret_key_input.text().strip() or None,
            aws_session_token=self.session_token_input.text().strip() or None,
            profile_name=self.profile_input.text().strip() or None
        )

    def connect_to_s3(self):
        try:
            self.s3_service = S3Uploader(self._settings_from_form())
            self.log("Connected to S3 client")
            self.refresh_buckets()
        except Exception as exc:
            self.show_error(f"Unable to connect: {exc}")

    def refresh_buckets(self):
        if not self.s3_service:
            self.connect_to_s3()
            if not self.s3_service:
                return
        try:
            current_bucket = self.bucket_combo.currentText().strip()
            buckets = self.s3_service.list_buckets()
            self.bucket_combo.blockSignals(True)
            self.bucket_combo.clear()
            self.bucket_combo.addItems(buckets)
            if current_bucket:
                self.bucket_combo.setCurrentText(current_bucket)
            self.bucket_combo.blockSignals(False)
            self.log(f"Loaded {len(buckets)} buckets")
            self.refresh_s3_browser()
        except Exception as exc:
            self.show_error(f"Unable to list buckets: {exc}")

    def create_bucket(self):
        if not self.s3_service:
            self.connect_to_s3()
            if not self.s3_service:
                return
        bucket_name = self.bucket_combo.currentText().strip()
        if not bucket_name:
            bucket_name, accepted = QInputDialog.getText(self, "Create Bucket", "Bucket name")
            if not accepted:
                return
            bucket_name = bucket_name.strip()
        if not bucket_name:
            self.show_error("Enter a bucket name first.")
            return
        try:
            self.s3_service.create_bucket(bucket_name)
            self.bucket_combo.setCurrentText(bucket_name)
            self.log(f"Created or confirmed bucket {bucket_name}")
            self.refresh_buckets()
        except Exception as exc:
            self.show_error(f"Unable to create bucket: {exc}")

    def _on_bucket_changed(self, _bucket_name):
        self.current_prefix = ""
        self.refresh_s3_browser()

    def refresh_s3_browser(self):
        if not self.s3_service:
            self.refresh_lifecycle_panel()
            return
        bucket_name = self.bucket_combo.currentText().strip()
        if not bucket_name:
            self.refresh_lifecycle_panel()
            return
        try:
            listing = self.s3_service.list_prefix(bucket_name, self.current_prefix)
            self.s3_tree.clear()
            self.current_path_label.setText(f"Current prefix: /{self.current_prefix}")
            for folder in listing['folders']:
                item = QTreeWidgetItem([folder['name'], 'Folder', ''])
                item.setData(0, Qt.UserRole, folder['prefix'])
                item.setData(1, Qt.UserRole, 'folder')
                self.s3_tree.addTopLevelItem(item)
            for file_info in listing['files']:
                item = QTreeWidgetItem([
                    file_info['name'],
                    'File',
                    S3Uploader._format_size(file_info['size'])
                ])
                item.setData(0, Qt.UserRole, file_info['key'])
                item.setData(1, Qt.UserRole, 'file')
                self.s3_tree.addTopLevelItem(item)
            self.update_s3_action_buttons()
            self.refresh_lifecycle_panel()
        except Exception as exc:
            self.show_error(f"Unable to browse bucket: {exc}")

    def refresh_lifecycle_panel(self):
        if not self.s3_service:
            self.lifecycle_output.setPlainText("Connect to S3 to view lifecycle policies.")
            return
        bucket_name = self.bucket_combo.currentText().strip()
        if not bucket_name:
            self.lifecycle_output.setPlainText("Select a bucket to view lifecycle policies.")
            return
        try:
            summary = self.s3_service.describe_lifecycle_policy(bucket_name)
            self.lifecycle_output.setPlainText(summary)
        except Exception as exc:
            self.lifecycle_output.setPlainText(
                f"Unable to load lifecycle policies for {bucket_name}: {exc}"
            )

    def update_s3_action_buttons(self):
        current_item = self.s3_tree.currentItem()
        if current_item is None:
            self.download_object_button.setEnabled(False)
            self.delete_object_button.setEnabled(False)
            self.delete_folder_button.setEnabled(False)
            return
        item_type = current_item.data(1, Qt.UserRole)
        self.download_object_button.setEnabled(item_type == 'file')
        self.delete_object_button.setEnabled(item_type == 'file')
        self.delete_folder_button.setEnabled(item_type == 'folder')

    def open_s3_item(self, item, _column):
        item_type = item.data(1, Qt.UserRole)
        if item_type == 'folder':
            self.current_prefix = item.data(0, Qt.UserRole)
            self.refresh_s3_browser()

    def navigate_up(self):
        prefix = self.current_prefix.rstrip('/')
        if not prefix:
            return
        parts = prefix.split('/')[:-1]
        self.current_prefix = "/".join(parts)
        if self.current_prefix:
            self.current_prefix += "/"
        self.refresh_s3_browser()

    def create_folder(self):
        if not self.s3_service:
            self.connect_to_s3()
            if not self.s3_service:
                return
        bucket_name = self.bucket_combo.currentText().strip()
        if not bucket_name:
            self.show_error("Choose a bucket first.")
            return
        folder_name, accepted = QInputDialog.getText(self, "Create Folder", "Folder name")
        if not accepted or not folder_name.strip():
            return
        prefix = f"{self.current_prefix}{folder_name.strip()}"
        try:
            created_prefix = self.s3_service.create_folder(bucket_name, prefix)
            self.log(f"Created folder {created_prefix}")
            self.refresh_s3_browser()
        except Exception as exc:
            self.show_error(f"Unable to create folder: {exc}")

    def _selected_s3_item(self):
        current_item = self.s3_tree.currentItem()
        if current_item is None:
            self.show_error("Select an S3 item first.")
            return None, None, None
        return (
            current_item,
            current_item.data(1, Qt.UserRole),
            current_item.data(0, Qt.UserRole)
        )

    def download_selected_object(self):
        if not self.s3_service:
            self.connect_to_s3()
            if not self.s3_service:
                return
        bucket_name = self.bucket_combo.currentText().strip()
        if not bucket_name:
            self.show_error("Choose a bucket first.")
            return
        current_item, item_type, key = self._selected_s3_item()
        if current_item is None:
            return
        if item_type != 'file':
            self.show_error("Select an object to download.")
            return
        target_path, _ = QFileDialog.getSaveFileName(
            self,
            "Download Object",
            current_item.text(0)
        )
        if not target_path:
            return
        try:
            saved_path = self.s3_service.download_object(bucket_name, key, target_path)
            self.log(f"Downloaded object {key} to {saved_path}")
        except Exception as exc:
            self.show_error(f"Unable to download object: {exc}")

    def delete_selected_object(self):
        if not self.s3_service:
            self.connect_to_s3()
            if not self.s3_service:
                return
        bucket_name = self.bucket_combo.currentText().strip()
        if not bucket_name:
            self.show_error("Choose a bucket first.")
            return
        current_item, item_type, key = self._selected_s3_item()
        if current_item is None:
            return
        if item_type != 'file':
            self.show_error("Select an object to delete.")
            return
        item_name = current_item.text(0)
        confirmation = QMessageBox.question(
            self,
            "Delete Object",
            f"Delete object '{item_name}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if confirmation != QMessageBox.Yes:
            return
        try:
            self.s3_service.delete_object(bucket_name, key)
            self.log(f"Deleted object {key}")
            self.refresh_s3_browser()
        except Exception as exc:
            self.show_error(f"Unable to delete object: {exc}")

    def delete_selected_folder(self):
        if not self.s3_service:
            self.connect_to_s3()
            if not self.s3_service:
                return
        bucket_name = self.bucket_combo.currentText().strip()
        if not bucket_name:
            self.show_error("Choose a bucket first.")
            return
        current_item, item_type, key = self._selected_s3_item()
        if current_item is None:
            return
        if item_type != 'folder':
            self.show_error("Select a folder to delete.")
            return
        item_name = current_item.text(0)
        confirmation = QMessageBox.question(
            self,
            "Delete Folder",
            f"Delete folder '{item_name}' and all its contents?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if confirmation != QMessageBox.Yes:
            return
        try:
            self.s3_service.delete_prefix(bucket_name, key)
            self.log(f"Deleted folder {key}")
            self.refresh_s3_browser()
        except Exception as exc:
            self.show_error(f"Unable to delete folder: {exc}")

    def _selected_local_paths(self):
        raw_paths = []
        for index in self.local_tree.selectionModel().selectedRows():
            raw_paths.append(Path(self.local_model.filePath(index)).resolve())
        if not raw_paths:
            return []
        unique_paths = sorted(set(raw_paths), key=lambda path: len(path.parts))
        filtered_paths = []
        for path in unique_paths:
            if any(parent in path.parents for parent in filtered_paths):
                continue
            filtered_paths.append(path)
        return [str(path) for path in filtered_paths]

    def update_local_preview(self, *_args):
        selected_paths = self._selected_local_paths()
        if not selected_paths:
            self.preview_image_path = None
            self.preview_caption.setText("Select a local image to preview.")
            self.preview_image.setPixmap(QPixmap())
            self.preview_image.setText("No image selected")
            return

        selected_path = Path(selected_paths[0])
        if not selected_path.is_file():
            self.preview_image_path = None
            self.preview_caption.setText(f"{selected_path.name} is a folder.")
            self.preview_image.setPixmap(QPixmap())
            self.preview_image.setText("Folder preview not available")
            return

        if selected_path.suffix.lower() not in self.IMAGE_EXTENSIONS:
            self.preview_image_path = None
            self.preview_caption.setText(f"{selected_path.name} is not a supported image format.")
            self.preview_image.setPixmap(QPixmap())
            self.preview_image.setText("No image preview available")
            return

        self.preview_image_path = selected_path
        self.preview_caption.setText(selected_path.name)
        self._render_preview_pixmap()

    def _render_preview_pixmap(self):
        if not self.preview_image_path:
            return
        pixmap = QPixmap(str(self.preview_image_path))
        if pixmap.isNull():
            self.preview_image.setPixmap(QPixmap())
            self.preview_image.setText("Unable to load image preview")
            return
        scaled = pixmap.scaled(
            self.preview_image.size(),
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation
        )
        self.preview_image.setText("")
        self.preview_image.setPixmap(scaled)

    def add_selected_to_queue(self):
        selected_paths = self._selected_local_paths()
        if not selected_paths:
            self.show_error("Select at least one local file or folder to queue.")
            return
        added = 0
        for path in selected_paths:
            if path in self.queued_paths:
                continue
            self.queued_paths.append(path)
            self.queue_list.addItem(path)
            added += 1
        self.log(f"Added {added} item(s) to the queue")

    def remove_selected_from_queue(self):
        selected_items = self.queue_list.selectedItems()
        if not selected_items:
            self.show_error("Select queued items to remove.")
            return
        removed = 0
        for item in selected_items:
            path = item.text()
            if path in self.queued_paths:
                self.queued_paths.remove(path)
            self.queue_list.takeItem(self.queue_list.row(item))
            removed += 1
        self.log(f"Removed {removed} item(s) from the queue")

    def clear_queue(self):
        self.queued_paths = []
        self.queue_list.clear()
        self.log("Cleared upload queue")

    def upload_selected(self):
        if self.upload_thread:
            self.show_error("An upload is already running.")
            return
        bucket_name = self.bucket_combo.currentText().strip()
        if not bucket_name:
            self.show_error("Choose or enter a bucket name first.")
            return
        if not self.queued_paths:
            self.show_error("Add at least one local file or folder to the queue first.")
            return

        self.progress_bar.setValue(0)
        self.status_label.setText("Transfer in progress...")
        self.upload_button.setEnabled(False)
        self.add_to_queue_button.setEnabled(False)
        self.remove_from_queue_button.setEnabled(False)
        self.clear_queue_button.setEnabled(False)

        settings = self._settings_from_form(bucket_name=bucket_name)
        prefix = self.prefix_input.text().strip() or self.current_prefix
        self.upload_thread = QThread(self)
        self.upload_worker = UploadWorker(settings, list(self.queued_paths), bucket_name, prefix)
        self.upload_worker.moveToThread(self.upload_thread)
        self.upload_thread.started.connect(self.upload_worker.run)
        self.upload_worker.progress.connect(self.on_upload_progress)
        self.upload_worker.finished.connect(self.on_upload_finished)
        self.upload_worker.failed.connect(self.on_upload_failed)
        self.upload_worker.finished.connect(self.upload_thread.quit)
        self.upload_worker.failed.connect(self.upload_thread.quit)
        self.upload_thread.finished.connect(self._cleanup_upload_thread)
        self.upload_thread.start()

    def on_upload_progress(self, event_type, payload):
        if event_type == 'summary':
            self.progress_bar.setMaximum(max(payload['files'], 1))
            self.progress_bar.setValue(0)
            self.log(
                f"Uploading {payload['files']} files to s3://{payload['bucket']}/{payload['prefix']}"
            )
            return
        if event_type == 'file_started':
            self.status_label.setText(f"Uploading {payload['filename']}")
            self.log(
                f"[{payload['index']}/{payload['total']}] Starting {payload['filename']}"
            )
            return
        if event_type == 'multipart_resumed':
            self.log(
                f"[{payload['index']}/{payload['total']}] Resuming {payload['filename']} "
                f"from part {payload['next_part_number']}"
            )
            return
        if event_type == 'part_progress':
            self.status_label.setText(
                f"{payload['filename']} part {payload['part_number']}/{payload['part_total']}"
            )
            self.log(
                f"[{payload['index']}/{payload['total']}] {payload['filename']} "
                f"{payload['completed_pct']:.2f}%"
            )
            return
        if event_type == 'file_skipped':
            self.progress_bar.setValue(payload['index'])
            self.log(
                f"[{payload['index']}/{payload['total']}] Skipped {payload['filename']}: {payload['message']}"
            )
            return
        if event_type == 'file_completed':
            self.progress_bar.setValue(payload['index'])
            self.log(
                f"[{payload['index']}/{payload['total']}] Completed {payload['filename']}"
            )

    def on_upload_finished(self, result):
        self.status_label.setText("Transfer complete")
        self.log(
            f"Transfer complete: {result['files']} files, {S3Uploader._format_size(result['bytes'])}"
        )
        self.clear_queue()
        self.upload_button.setEnabled(True)
        self.add_to_queue_button.setEnabled(True)
        self.remove_from_queue_button.setEnabled(True)
        self.clear_queue_button.setEnabled(True)
        self.refresh_s3_browser()

    def on_upload_failed(self, message):
        self.status_label.setText("Transfer failed")
        self.upload_button.setEnabled(True)
        self.add_to_queue_button.setEnabled(True)
        self.remove_from_queue_button.setEnabled(True)
        self.clear_queue_button.setEnabled(True)
        self.show_error(message)

    def _cleanup_upload_thread(self):
        if self.upload_worker:
            self.upload_worker.deleteLater()
        if self.upload_thread:
            self.upload_thread.deleteLater()
        self.upload_worker = None
        self.upload_thread = None

    def log(self, message):
        self.log_output.append(message)

    def show_error(self, message):
        self.log(f"ERROR: {message}")
        QMessageBox.critical(self, "S3 Uploader", message)


def main():
    if QApplication is None:  # pragma: no cover - runtime dependency
        raise SystemExit(
            "PySide6 is required to run the desktop UI. "
            f"Import error: {QT_IMPORT_ERROR}"
        )

    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    app = QApplication([])
    window = S3UploaderWindow()
    window.show()
    app.exec()


if __name__ == '__main__':
    main()
