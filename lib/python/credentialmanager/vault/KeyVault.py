### PostgreSQL Key, SGDAKey, Local Key
import inspect
import os
from sqlalchemy.engine.url import make_url
from .AbstractKeyVault import *

class LocalKeyVault(AbstractKeyVault):
    """
    Local Key vault
    based on SQLite
    """

    def __init__(self, owner, table_name=None):
        self.sqlite_file = owner.conn_db
        url = make_url(self.sqlite_file)
        if url.drivername == 'sqlite' and url.database !='./.credentials/localVault.sqlite':
            conn_str = self.sqlite_file
        else:
            self.sqlite_file = './.credentials/localVault.sqlite'
            os.makedirs(os.path.dirname(self.sqlite_file), exist_ok=True)
            conn_str = 'sqlite:///'+self.sqlite_file
        if table_name is None:
            super(LocalKeyVault, self).__init__(owner, conn_str)
        else:
            super(LocalKeyVault, self).__init__(owner, conn_str, table_name=table_name)
            

class PSQLKeyVault(AbstractKeyVault):
    """
    PostgreSQL Key vault
    """

    def __init__(self, owner, credential):
        self.setId("pgsql_keyVault")
        # create the connection string for this credential storage
        conn_str = "postgresql+psycopg2://{}:{}@{}/{}".format(credential.content['username'], credential.content['password'],
                                                   credential.content['host'], credential.content['database'])

        if not credential.hasKey("table"):
            super().__init__(owner, conn_str)
        else:
            super().__init__(owner, conn_str, table_name="key_chain")
        
        
# final key vaults need to be created by inheriting a KeyVault class
class SGDAKeyVault(PSQLKeyVault):
    def __init__(self, owner):
        self.setId("sgda_key_vault")
        # get the credential from the current vault in owner
        credential = owner.retrieve("sgda_vault_credential", False, None)
        super().__init__(owner, credential)
