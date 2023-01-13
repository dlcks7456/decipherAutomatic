import decipherAutomatic.utils.key_variables as key
import decipherAutomatic.utils.system_variables  as sys

sys_vars = [i for i in sys.variables.split('\n') if not i == '']
key_vars = [i for i in key.variables.split('\n') if not i == '']
