"""
Test básico para el paquete aclimate_v3_historical_location_etl
"""


def test_import():
    """Test que verifica que el paquete se puede importar correctamente"""
    try:
        import aclimate_v3_historical_location_etl

        assert True
    except ImportError:
        assert False, "No se pudo importar el paquete"


def test_basic_functionality():
    """Test básico de funcionalidad"""
    # Aquí puedes agregar tests específicos de tu ETL
    assert 1 + 1 == 2  # Test dummy para empezar
