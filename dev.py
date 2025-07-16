#!/usr/bin/env python3
"""
Script de ayuda para el desarrollo del proyecto AClimate v3 Historical Location ETL
"""
import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, description):
    """Ejecuta un comando y muestra el resultado"""
    print(f"\n🔧 {description}")
    print(f"▶️  {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✅ {description} - EXITOSO")
        if result.stdout:
            print(result.stdout)
    else:
        print(f"❌ {description} - ERROR")
        if result.stderr:
            print(result.stderr)
        return False
    return True

def setup_dev_environment():
    """Configura el entorno de desarrollo"""
    print("🚀 Configurando entorno de desarrollo...")
    
    commands = [
        ("python -m pip install --upgrade pip", "Actualizando pip"),
        ("pip install -e .", "Instalando paquete con dependencias básicas"),
        ("pip install -e .[dev]", "Instalando dependencias de desarrollo"),
        ("pre-commit install", "Instalando pre-commit hooks"),
    ]
    
    for cmd, desc in commands:
        if not run_command(cmd, desc):
            return False
    
    print("\n🎉 Entorno de desarrollo configurado exitosamente!")
    print("💡 Para configurar SigNoz, copia .env.example a .env y configura las variables")
    return True

def run_tests():
    """Ejecuta los tests"""
    print("🧪 Ejecutando tests...")
    
    commands = [
        ("pytest tests/ -v", "Ejecutando tests"),
        ("pytest tests/ --cov=src/ --cov-report=html", "Ejecutando tests con coverage"),
    ]
    
    for cmd, desc in commands:
        if not run_command(cmd, desc):
            return False
    
    print("\n📊 Coverage report generado en htmlcov/index.html")
    return True

def run_quality_checks():
    """Ejecuta verificaciones de calidad de código"""
    print("🔍 Ejecutando verificaciones de calidad...")
    
    commands = [
        ("black --check src/ tests/", "Verificando formateo con black"),
        ("flake8 src/ tests/", "Verificando estilo con flake8"),
        ("mypy src/", "Verificando tipos con mypy"),
    ]
    
    for cmd, desc in commands:
        if not run_command(cmd, desc):
            return False
    
    print("\n✨ Todas las verificaciones de calidad pasaron!")
    return True

def format_code():
    """Formatea el código"""
    print("🎨 Formateando código...")
    
    commands = [
        ("black src/ tests/", "Formateando con black"),
        ("isort src/ tests/", "Ordenando imports con isort"),
    ]
    
    for cmd, desc in commands:
        run_command(cmd, desc)
    
    print("\n✨ Código formateado exitosamente!")

def build_package():
    """Construye el paquete"""
    print("📦 Construyendo paquete...")
    
    commands = [
        ("python -m build", "Construyendo paquete"),
        ("python -m twine check dist/*", "Verificando paquete"),
    ]
    
    for cmd, desc in commands:
        if not run_command(cmd, desc):
            return False
    
    print("\n📦 Paquete construido exitosamente!")
    return True

def clean():
    """Limpia archivos generados"""
    print("🧹 Limpiando archivos generados...")
    
    patterns = [
        "build/",
        "dist/",
        "*.egg-info/",
        "__pycache__/",
        ".pytest_cache/",
        ".coverage",
        "htmlcov/",
        ".mypy_cache/",
    ]
    
    for pattern in patterns:
        cmd = f"rm -rf {pattern}" if os.name != 'nt' else f"rmdir /s /q {pattern} 2>nul || del /s /q {pattern} 2>nul || echo."
        subprocess.run(cmd, shell=True, capture_output=True)
    
    print("✅ Archivos de build limpiados!")

def show_help():
    """Muestra ayuda"""
    print("""
🚀 Script de ayuda para AClimate v3 Historical Location ETL

Comandos disponibles:
  install   - Instala las dependencias del proyecto
  setup     - Configura el entorno de desarrollo
  test      - Ejecuta los tests
  quality   - Ejecuta verificaciones de calidad
  format    - Formatea el código
  build     - Construye el paquete
  clean     - Limpia archivos generados
  help      - Muestra esta ayuda

Ejemplos:
  python dev.py install
  python dev.py setup
  python dev.py test
  python dev.py quality
  python dev.py format
  python dev.py build
  python dev.py clean
    """)

def install_dependencies():
    """Instala las dependencias del proyecto"""
    print("📦 Instalando dependencias del proyecto...")
    
    commands = [
        ("python -m pip install --upgrade pip", "Actualizando pip"),
        ("pip install -r requirements.txt", "Instalando dependencias desde requirements.txt"),
        ("pip install -e .", "Instalando el paquete en modo desarrollo"),
    ]
    
    for cmd, desc in commands:
        if not run_command(cmd, desc):
            return False
    
    print("\n✅ Dependencias instaladas exitosamente!")
    return True

def main():
    """Función principal"""
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == "install":
        install_dependencies()
    elif command == "setup":
        setup_dev_environment()
    elif command == "test":
        run_tests()
    elif command == "quality":
        run_quality_checks()
    elif command == "format":
        format_code()
    elif command == "build":
        build_package()
    elif command == "clean":
        clean()
    elif command == "help":
        show_help()
    else:
        print(f"❌ Comando desconocido: {command}")
        show_help()

if __name__ == "__main__":
    main()
