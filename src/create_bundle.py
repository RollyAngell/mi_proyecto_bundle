from databricks.sdk.runtime import *
from databricks.sdk.service.catalog import AssetBundle
from config.bundle_config import BUNDLE_CONFIG

def crear_bundle():
    """Crea y despliega el asset bundle"""
    try:
        bundle = AssetBundle.from_dict(BUNDLE_CONFIG)
        bundle.deploy()
        return bundle
    except Exception as e:
        print(f"Error al crear el bundle: {str(e)}")
        return None 