import logging
from pathlib import Path

from s3uploader_core import S3Uploader
from s3uploader_core import S3UploaderSettings

try:
    from PySide6.QtCore import QObject
    from PySide6.QtCore import QDir
    from PySide6.QtCore import QThread
    from PySide6.QtCore import Qt
    from PySide6.QtCore import Signal
    from PySide6.QtWidgets import QApplication
    from PySide6.QtWidgets import QFileSystemModel
    from PySide6.QtWidgets import QFormLayout
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
    def __init__(self):
        super().__init__()
        self.setWindowTitle("S3 Uploader")
        self.resize(1500, 900)
        self.s3_service = None
        self.current_prefix = ""
        self.upload_thread = None
        self.upload_worker = None
        self.queued_paths = []
        self._build_ui()
        self._connect_signals()
        self._set_local_root(QDir.homePath())

    def _build_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        root_layout = QVBoxLayout(central_widget)

        settings_layout = QHBoxLayout()
        root_layout.addLayout(settings_layout)

        credentials_layout = QFormLayout()
        self.access_key_input = QLineEdit()
        self.access_key_input.setEchoMode(QLineEdit.Password)
        self.secret_key_input = QLineEdit()
        self.secret_key_input.setEchoMode(QLineEdit.Password)
        self.session_token_input = QLineEdit()
        self.session_token_input.setEchoMode(QLineEdit.Password)
        self.region_input = QLineEdit("eu-west-1")
        self.endpoint_input = QLineEdit()
        self.endpoint_input.setPlaceholderText("Optional for AWS; leave blank")
        self.profile_input = QLineEdit()
        credentials_layout.addRow("Access Key", self.access_key_input)
        credentials_layout.addRow("Secret Key", self.secret_key_input)
        credentials_layout.addRow("Session Token", self.session_token_input)
        credentials_layout.addRow("Region", self.region_input)
        credentials_layout.addRow("Endpoint URL", self.endpoint_input)
        credentials_layout.addRow("Profile", self.profile_input)
        settings_layout.addLayout(credentials_layout, 3)

        bucket_layout = QFormLayout()
        self.bucket_combo = QComboBox()
        self.bucket_combo.setEditable(True)
        self.prefix_input = QLineEdit()
        self.chunk_size_input = QLineEdit("64")
        self.connect_button = QPushButton("Connect")
        self.refresh_buckets_button = QPushButton("Refresh Buckets")
        self.create_bucket_button = QPushButton("Create Bucket")
        self.refresh_s3_button = QPushButton("Refresh S3")
        bucket_layout.addRow("Bucket", self.bucket_combo)
        bucket_layout.addRow("Target Prefix", self.prefix_input)
        bucket_layout.addRow("Chunk Size MB", self.chunk_size_input)
        bucket_layout.addRow(self.connect_button, self.refresh_buckets_button)
        bucket_layout.addRow(self.create_bucket_button, self.refresh_s3_button)
        settings_layout.addLayout(bucket_layout, 2)

        splitter = QSplitter(Qt.Horizontal)
        root_layout.addWidget(splitter, 1)

        local_panel = QWidget()
        local_layout = QVBoxLayout(local_panel)
        local_layout.addWidget(QLabel("Local Files"))
        self.local_model = QFileSystemModel(self)
        self.local_model.setRootPath(QDir.rootPath())
        self.local_model.setFilter(QDir.AllDirs | QDir.Files | QDir.NoDotAndDotDot)
        self.local_tree = QTreeView()
        self.local_tree.setModel(self.local_model)
        self.local_tree.setSelectionMode(QTreeView.ExtendedSelection)
        self.local_tree.setSortingEnabled(True)
        for column in range(1, 4):
            self.local_tree.hideColumn(column)
        local_layout.addWidget(self.local_tree, 1)
        queue_button_layout = QHBoxLayout()
        self.add_to_queue_button = QPushButton("Add To Queue")
        self.remove_from_queue_button = QPushButton("Remove From Queue")
        self.clear_queue_button = QPushButton("Clear Queue")
        queue_button_layout.addWidget(self.add_to_queue_button)
        queue_button_layout.addWidget(self.remove_from_queue_button)
        queue_button_layout.addWidget(self.clear_queue_button)
        local_layout.addLayout(queue_button_layout)
        queue_header_layout = QHBoxLayout()
        queue_header_layout.addWidget(QLabel("Transfer Queue"))
        self.upload_button = QPushButton("Start Transfer")
        self.upload_button.setMinimumHeight(44)
        queue_header_layout.addWidget(self.upload_button)
        local_layout.addLayout(queue_header_layout)
        self.queue_list = QListWidget()
        self.queue_list.setSelectionMode(QListWidget.ExtendedSelection)
        local_layout.addWidget(self.queue_list, 1)
        splitter.addWidget(local_panel)

        s3_panel = QWidget()
        s3_layout = QVBoxLayout(s3_panel)
        s3_layout.addWidget(QLabel("S3 Browser"))
        self.current_path_label = QLabel("Current prefix: /")
        s3_layout.addWidget(self.current_path_label)
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
        s3_layout.addLayout(s3_actions_layout)
        self.s3_tree = QTreeWidget()
        self.s3_tree.setColumnCount(3)
        self.s3_tree.setHeaderLabels(["Name", "Type", "Size"])
        s3_layout.addWidget(self.s3_tree, 1)
        self.s3_up_button = QPushButton("Up One Level")
        s3_layout.addWidget(self.s3_up_button)
        splitter.addWidget(s3_panel)
        splitter.setSizes([700, 700])

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

        root_layout.addWidget(QLabel("Logs"))
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        root_layout.addWidget(self.log_output, 1)

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
        self.add_to_queue_button.clicked.connect(self.add_selected_to_queue)
        self.remove_from_queue_button.clicked.connect(self.remove_selected_from_queue)
        self.clear_queue_button.clicked.connect(self.clear_queue)
        self.upload_button.clicked.connect(self.upload_selected)

    def _set_local_root(self, path):
        root_index = self.local_model.index(path)
        if root_index.isValid():
            self.local_tree.setRootIndex(root_index)

    def _settings_from_form(self, bucket_name=None):
        chunk_size = int(self.chunk_size_input.text().strip() or S3UploaderSettings.DEFAULT_CHUNK_SIZE)
        return S3UploaderSettings(
            bucket_name=bucket_name,
            s3_prefix=self.prefix_input.text().strip(),
            chunk_size_mb=chunk_size,
            endpoint_url=self.endpoint_input.text().strip() or None,
            region_name=self.region_input.text().strip() or None,
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
            return
        bucket_name = self.bucket_combo.currentText().strip()
        if not bucket_name:
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
        except Exception as exc:
            self.show_error(f"Unable to browse bucket: {exc}")

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
