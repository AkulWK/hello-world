class Car:
    def __init__(self, ime, expertiza):
        self.name = ime
        self.skill = expertiza
        if expertiza == "kesh":
            print(f"Cao,{self.name}, bice tebi do jaja!")
    
    def sljaka(self):
        print(f"sljaka {self.name}, bez brige")

if __name__ == "__main__":
    car = Car("Luka", "keh")
    car.sljaka()
    print(car.name)
    print(car.skill)
    print(car.sljaka)