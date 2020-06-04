from arquivos.trata import Request
import os.path


def main_req():
    Request(open(os.path.join("config", "config.json")))


if __name__ == "__main__":
    Request(open(os.path.join("config", "config.json")))
