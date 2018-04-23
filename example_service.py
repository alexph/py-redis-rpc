from pyredisrpc.rpc import rpc, Service, Container


class EchoService(Service):
    @rpc
    def echo(self, string_input):
        return string_input


container = Container([
    EchoService('echo')
], redis_url='redis:///2')


if __name__ == '__main__':
    container.run()
