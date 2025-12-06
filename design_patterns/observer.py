from abc import ABC, abstractmethod

class Observer(ABC):
    @abstractmethod
    def update(self, data: dict[str, str]):
        pass


class Observable(ABC):
    @abstractmethod
    def attach(self, observer: Observer):
        pass

    @abstractmethod
    def detach(self, observer: Observer):
        pass

    @abstractmethod
    def notify(self):
        pass


class Display(ABC):
    @abstractmethod
    def display(self, data: dict[str, str]):
        pass


class LoggingDisplay(Display):
    def display(self, data: dict[str, str]):
        for key, value in data.items():
            print(key.title(), ": ", value)


class SubscriberA(Observer, LoggingDisplay):
    def __init__(self, subject: Observable) -> None:
        self.subject = subject
        self.data = {}
        subject.attach(self)

    def update(self, data: dict[str, str]):
        for key, value in data.items():
            data[key] = f"A: {value}"

        self.data = data
        self.display(data=data)

    def detach(self):
        self.subject.detach(self)


class SubscriberB(Observer, LoggingDisplay):
    def __init__(self, subject: Observable) -> None:
        self.subject = subject
        self.data = {}
        subject.attach(self)

    def update(self, data: dict[str, str]):
        for key, value in data.items():
            data[key] = f"B: {value}"

        self.data = data
        self.display(data=data)

    def detach(self):
        self.subject.detach(self)


class PublisherA(Observable):
    def __init__(self) -> None:
        self.subscribers: list[Observer] = []
        self.value: int = 0
        
    def attach(self, observer: Observer):
        self.subscribers.append(observer)

    def detach(self, observer: Observer):
        self.subscribers.remove(observer)

    def notify(self):
        for sub in self.subscribers:
            sub.update(data={
                "value": str(self.value)
            })
        print("-----")

    def update(self, value):
        self.value = value
        self.notify()


publisher = PublisherA()
subscribera = SubscriberA(publisher)
subscriberb = SubscriberB(publisher)
publisher.update(10)
publisher.update(20)
subscribera.detach()
publisher.update(30)
