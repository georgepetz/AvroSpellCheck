import avro.schema, sys, json, re

SNAKE_CASE_TEST_RE = re.compile(r'^[a-z]+([a-z\d]+_|_[a-z]+)+[a-z\d]+$')
SNAKE_CASE_TEST_DASH_RE = re.compile(r'^[a-z]+([a-z\d]+-|-[a-z]+)+[a-z\d]+$')
ONE_WORD = re.compile(r'^[a-z]+$')

def is_string(obj):
    try:  # basestring is available in python 2 but missing in python 3!
        return isinstance(obj, basestring)
    except NameError:
        return isinstance(obj, str)

def is_full_string(string):
    return is_string(string) and string.strip() != ''

def is_snake_case(string, separator='_'):
    if is_full_string(string):
        if len(string.replace("_", " ").split()) == 1:
            return bool(re.search(ONE_WORD, string))
        else:
            pass
            re_map = {
                '_': SNAKE_CASE_TEST_RE,
                '-': SNAKE_CASE_TEST_DASH_RE
            }
            re_template = '^[a-z]+([a-z\d]+{sign}|{sign}[a-z]+)+[a-z\d]+$'
            r = re_map.get(separator, re.compile(re_template.format(sign=re.escape(separator))))
            return bool(r.search(string))
    return False



print(">>>>> Validando extensão")
if sys.argv[1].endswith(".avsc"):
    print(" >>>>> Extensão de arquivo válido")
else:
    print(" >>>>> Extensão de arquivo inválido")
    sys.exit(1)

print(">>>>> Validando schema")
try:
    schema = avro.schema.Parse(open(sys.argv[1], "rb").read())
except:
    print(" >>>>> Formato do schema inválido")
    sys.exit(2)
print(" >>>>> Formato do schema válido")

data = json.loads(str(schema))
fields = data['fields']

print(">>>>> Validando campos snake_case")
foraPadrao = ""
for field in fields:
    if is_snake_case(field['name']):
        continue
    else:
        foraPadrao = foraPadrao + "["+ field['name'] +"] "
if foraPadrao != "":
    print(" >>>>> Campos fora do padrão snake_case: " + foraPadrao)
    sys.exit(3)
else:
    print(" >>>>> Campos dentro do padrão snake_case")



