from abc import ABC, abstractmethod

class Beverage(ABC):
    @abstractmethod
    def get_description(self) -> str:
        pass

    @abstractmethod
    def get_cost(self) -> float:
        pass

class CondimentDecorator(Beverage):
    def __init__(self, beverage: Beverage) -> None:
        self._beverage = beverage

    @abstractmethod
    def get_description(self) -> str:
        pass

# Beverage Implementations
class Espresso(Beverage):
    def __init__(self) -> None:
        self.description = "Espresso"
        self.cost = 4.0

    def get_description(self) -> str:
        return self.description
    
    def get_cost(self) -> float:
        return self.cost


class HouseBlend(Beverage):
    def __init__(self) -> None:
        self.description = "House Blend"
        self.cost = 5.0

    def get_description(self) -> str:
        return self.description
    
    def get_cost(self) -> float:
        return self.cost


# Condiments
class Mocha(CondimentDecorator):
    def __init__(self, beverage: Beverage) -> None:
        super().__init__(beverage)

    def get_description(self) -> str:
        return self._beverage.get_description() + ", Mocha"

    def get_cost(self) -> float:
        return self._beverage.get_cost() + 1.0


class Whip(CondimentDecorator):
    def __init__(self, beverage: Beverage) -> None:
        super().__init__(beverage)

    def get_description(self) -> str:
        return self._beverage.get_description() + ", Whip"

    def get_cost(self) -> float:
        return self._beverage.get_cost() + 2.0


espresso = Espresso()
mocha_espresso = Mocha(espresso)
whip_mocha_espresso = Whip(mocha_espresso)

print(espresso.get_description(), ":", espresso.get_cost())
print(mocha_espresso.get_description(), ":", mocha_espresso.get_cost())
print(whip_mocha_espresso.get_description(), ":", whip_mocha_espresso.get_cost())
