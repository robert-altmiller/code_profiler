class Inventory:
    def __init__(self):
        # Initializes the inventory with an empty dictionary
        self.items = {}

    def add_item(self, item_id, details):
        # Adds an item to the inventory
        if item_id in self.items:
            print("Item already exists.")
        else:
            self.items[item_id] = details
            print(f"Item {item_id} added.")

    def update_item(self, item_id, details):
        # Updates an item's details
        if item_id in self.items:
            self.items[item_id] = details
            print(f"Item {item_id} updated.")
        else:
            print("Item does not exist.")

    def remove_item(self, item_id):
        # Removes an item from the inventory
        if item_id in self.items:
            del self.items[item_id]
            print(f"Item {item_id} removed.")
        else:
            print("Item does not exist.")

    def search_item(self, item_id):
        # Searches for an item by ID
        return self.items.get(item_id, "Item does not exist.")

    def list_items(self):
        # Lists all items in the inventory
        return self.items