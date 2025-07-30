"""
Tests para el LoggingManager
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import patch

# Configuración de path para imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Imports del proyecto (después de configurar path)
from aclimate_v3_historical_location_etl.tools.logging_manager import (  # noqa: E402
    LoggingManager,
)


class TestLoggingManager(unittest.TestCase):
    """Test cases para LoggingManager"""

    def setUp(self):
        """Configuración antes de cada test"""
        # Crear un archivo temporal para logs
        self.temp_log_file = tempfile.NamedTemporaryFile(delete=False)
        self.temp_log_file.close()

    def tearDown(self):
        """Limpieza después de cada test"""
        # Cerrar todos los handlers para liberar el archivo
        if hasattr(self, "logging_manager"):
            for handler in self.logging_manager.logger.handlers[:]:
                handler.close()
                self.logging_manager.logger.removeHandler(handler)

        # Eliminar el archivo temporal
        if os.path.exists(self.temp_log_file.name):
            try:
                os.unlink(self.temp_log_file.name)
            except PermissionError:
                # En Windows, a veces necesita un poco más de tiempo
                import time

                time.sleep(0.1)
                try:
                    os.unlink(self.temp_log_file.name)
                except PermissionError:
                    pass  # Ignorar si no se puede eliminar

    def test_logging_manager_initialization(self):
        """Test que verifica la inicialización del LoggingManager"""
        service_name = "test_service"
        self.logging_manager = LoggingManager(
            service_name=service_name, log_file=self.temp_log_file.name
        )

        self.assertEqual(self.logging_manager.service_name, service_name)
        self.assertEqual(self.logging_manager.log_file, self.temp_log_file.name)
        self.assertIsNotNone(self.logging_manager.logger)

    def test_file_logging(self):
        """Test que verifica que los logs se escriben al archivo"""
        self.logging_manager = LoggingManager(
            service_name="test_service", log_file=self.temp_log_file.name
        )

        test_message = "Test log message"
        self.logging_manager.info(test_message, component="test_component")

        # Verificar que el mensaje se escribió al archivo
        with open(self.temp_log_file.name, "r") as f:
            content = f.read()
            self.assertIn(test_message, content)
            self.assertIn("test_service", content)

    def test_different_log_levels(self):
        """Test que verifica diferentes niveles de log"""
        self.logging_manager = LoggingManager(
            service_name="test_service", log_file=self.temp_log_file.name
        )

        self.logging_manager.info("Info message")
        self.logging_manager.warning("Warning message")
        self.logging_manager.error("Error message")
        self.logging_manager.debug("Debug message")

        # Verificar que los mensajes se escribieron al archivo
        with open(self.temp_log_file.name, "r") as f:
            content = f.read()
            self.assertIn("Info message", content)
            self.assertIn("Warning message", content)
            self.assertIn("Error message", content)
            # Debug puede no aparecer dependiendo del nivel configurado

    @patch.dict(os.environ, {"ENABLE_SIGNOZ": "false"})
    def test_signoz_disabled(self):
        """Test que verifica que SigNoz se deshabilita correctamente"""
        self.logging_manager = LoggingManager(
            service_name="test_service", log_file=self.temp_log_file.name
        )

        # SigNoz debe estar deshabilitado
        self.assertFalse(self.logging_manager._signoz_enabled)

    def test_log_with_extra_data(self):
        """Test que verifica logging con datos extra"""
        self.logging_manager = LoggingManager(
            service_name="test_service", log_file=self.temp_log_file.name
        )

        self.logging_manager.info(
            "Message with extra data",
            component="test_component",
            user_id="123",
            transaction_id="abc",
        )

        # Verificar que el mensaje se escribió al archivo
        with open(self.temp_log_file.name, "r") as f:
            content = f.read()
            self.assertIn("Message with extra data", content)

    def test_endpoint_availability_check(self):
        """Test que verifica la verificación de disponibilidad del endpoint"""
        self.logging_manager = LoggingManager(
            service_name="test_service", log_file=self.temp_log_file.name
        )

        # Test con endpoint que no existe
        result = self.logging_manager._is_endpoint_available("nonexistent:9999")
        self.assertFalse(result)

        # Test con endpoint malformado
        result = self.logging_manager._is_endpoint_available("invalid_endpoint")
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
