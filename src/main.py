from flask import Flask
import os

app = Flask(__name__)

# Exemplo de como ler uma variável de ambiente (ex: DATABASE_URL)
# db_url = os.environ.get('DATABASE_URL')

@app.route('/')
def hello_world():
    return 'Olá! Seu backend Flask está funcionando!'

@app.route('/health')
def health_check():
    # Aqui poderíamos adicionar uma verificação de conexão com o banco de dados
    return 'OK', 200

if __name__ == '__main__':
    # A App Platform usará o Gunicorn especificado no Procfile,
    # esta seção é mais para desenvolvimento local.
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
