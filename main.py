from neo4j import GraphDatabase
import csv

# Conexão com o banco de dados Neo4j
uri = "bolt://localhost:7687"
username = "luiz"
password = "l.aguiar5"

# Função para importar dados de um arquivo CSV para Neo4j
def importar_dados_csv(file_path, session):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row:
                # Aqui definimos o label do nó (node_label) conforme o nome do arquivo CSV
                # Por exemplo, produtos.csv resultará em (:Produto)
                node_label = f":{file_path.split('/')[-1].split('.')[0].capitalize()}"
                query = f"CREATE ({node_label} " + "{" + ", ".join([f"{key}: '{value}'" for key, value in row.items()]) + "})"
                session.run(query)

def main():
    # Conecta ao banco de dados Neo4j
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    with driver.session() as session:
        # Importa dados dos arquivos CSV
        importar_dados_csv('import/produtos.csv', session)
        importar_dados_csv('import/usuarios.csv', session)
        importar_dados_csv('import/recomendacoes.csv', session)
        importar_dados_csv('import/estoque.csv', session)

    # Fecha a conexão com o banco de dados Neo4j
    driver.close()

if __name__ == "__main__":
    main()
