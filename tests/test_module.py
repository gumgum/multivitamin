from multivitamin.module import Module


def test_prev_props_of_interest():
    class ConcreteModule(Module):
        def process(self, response):
            super().process(response)

    server_name = "Dummy"
    version = "1.0.0"
    cmod = ConcreteModule(server_name, version)
    pois = [{"property_type": "object", "value": "car"}]
    cmod.set_prev_props_of_interest(pois)
