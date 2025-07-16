"""
Tests para el LoggingManager
"""
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import sys
import logging

# Agregar el directorio src al path para importar el módulo
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from aclimate_v3_historical_location_etl.tools.logging_manager import LoggingManager


class TestLoggingManager(unittest.TestCase):
    """Test cases para LoggingManager"""

    def setUp(self):
        """Configuración antes de cada test"""
        # Crear un archivo temporal para logs
        self.temp_log_file = tempfile.NamedTemporaryFile(delete=False)
        self.temp_log_file.close()

    def tearDown(self):
        """Limpieza después de cada test"""
        # Eliminar el archivo temporal
        if os.path.exists(self.temp_log_file.name):
            os.unlink(self.temp_log_file.name)

    def test_logging_manager_initialization(self):
        """Test que verifica la inicialización del LoggingManager"""
        service_name = "test_service"
        log_manager = LoggingManager(
            service_name=service_name,
            log_file=self.temp_log_file.name
        )
        
        self.assertEqual(log_manager.service_name, service_name)
        self.assertEqual(log_manager.log_file, self.temp_log_file.name)
        self.assertIsNotNone(log_manager.logger)

    def test_file_logging(self):
        """Test que verifica que los logs se escriben al archivo"""
        log_manager = LoggingManager(
            service_name="test_service",
            log_file=self.temp_log_file.name
        )
        
        test_message = "Test log message"
        log_manager.info(test_message, component="test_component")
        
        # Verificar que el mensaje se escribió al archivo
        with open(self.temp_log_file.name, 'r') as f:
            content = f.read()
            self.assertIn(test_message, content)
            self.assertIn("test_service", content)

    def test_different_log_levels(self):
        """Test que verifica diferentes niveles de log"""
        log_manager = LoggingManager(
            service_name="test_service",
            log_file=self.temp_log_file.name
        )
        
        log_manager.info("Info message")
        log_manager.warning("Warning message")
        log_manager.error("Error message")
        log_manager.debug("Debug message")
        
        # Verificar que los mensajes se escribieron al archivo
        with open(self.temp_log_file.name, 'r') as f:
            content = f.read()
            self.assertIn("Info message", content)
            self.assertIn("Warning message", content)
            self.assertIn("Error message", content)
            # Debug puede no aparecer dependiendo del nivel configurado

    @patch.dict(os.environ, {'ENABLE_SIGNOZ': 'false'})
    def test_signoz_disabled(self):
        """Test que verifica que SigNoz se deshabilita correctamente"""
        log_manager = LoggingManager(
            service_name="test_service",
            log_file=self.temp_log_file.name
        )
        
        # SigNoz debe estar deshabilitado
        self.assertFalse(log_manager._signoz_enabled)

    def test_log_with_extra_data(self):
        """Test que verifica logging con datos extra"""
        log_manager = LoggingManager(
            service_name="test_service",
            log_file=self.temp_log_file.name
        )
        
        log_manager.info(
            "Message with extra data",
            component="test_component",
            user_id="123",
            transaction_id="abc"
        )
        
        # Verificar que el mensaje se escribió al archivo
        with open(self.temp_log_file.name, 'r') as f:
            content = f.read()
            self.assertIn("Message with extra data", content)

    def test_endpoint_availability_check(self):
        """Test que verifica la verificación de disponibilidad del endpoint"""
        log_manager = LoggingManager(
            service_name="test_service",
            log_file=self.temp_log_file.name
        )
        
        # Test con endpoint que no existe
        result = log_manager._is_endpoint_available("nonexistent:9999")
        self.assertFalse(result)
        
        # Test con endpoint malformado
        result = log_manager._is_endpoint_available("invalid_endpoint")
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
