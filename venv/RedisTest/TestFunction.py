import random

random.seed(777)
hats = {f"hat:{random.getrandbits(32)}": i for i in (
    {
        "color": "black",
        "price": 49.99,
        "style": "fitted",
        "quantity": 1000,
        "npurchased": 0,
    },
    {
        "color": "maroon",
        "price": 59.99,
        "style": "hipster",
        "quantity": 500,
        "npurchased": 0,
    },
    {
        "color": "green",
        "price": 99.99,
        "style": "baseball",
        "quantity": 200,
        "npurchased": 0,
    })
        }

# print(hats.items())
# for xuaaa, hat in hats.items():
#     print("{} {}".format(xuaaa, hat))


def insertHash_ne(i, pm_or_fm) -> None:
    ne = {f"ne_{pm_or_fm}:{i}":
        {
            "last_time": random.randint(0, 9),
            "alarm_init": "",
            "alarm_trigger": ""
        }
    }
    print(type(ne))
    print(ne.items())

list1 = [1, 2,2, 4]
list2 = []

# list_difference = [item for item in list1 if item not in list2]
set_difference = set(list1) - set(list2)
list_difference = list(set_difference)
set_difference2 = set(list2) - set(list1)
list_difference2 = list(set_difference2)

print(list_difference)
print(list_difference2)
