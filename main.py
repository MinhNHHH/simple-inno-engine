from engine.engine import InnoEngine
from memory.pages import Page

def main():
    engine = InnoEngine()
    # page1 = Page(
    #     page_id=1,
    #     rows=[
    #         (1, "Alice"),
    #         (2, "Bob"),
    #         (3, "Carol"),
    #         (4, "Dave")
    #     ],
    #     page_lsn=100
    # )
    # page2 = Page(
    #     page_id=2,
    #     rows=[
    #         (1, "Alice2"),
    #         (2, "Bob2"),
    #         (3, "Carol2"),
    #         (4, "Dave2")
    #     ],
    #     page_lsn=100
    # )
    # page3 = Page(
    #     page_id=3 ,
    #     rows=[
    #         (1, "Alice3"),
    #         (2, "Bob3"),
    #         (3, "Carol3"),
    #         (4, "Dave3")
    #     ],
    #     page_lsn=100
    # )

    data = [(1, "Bob"), (2, "Carol"), (3, "Dave"), (4, "Eve"), (5, "Frank"), (6, "George"), (7, "Harry"), (8, "Ivy"), (9, "Jack"), (10, "Lily"), (11, "Mason"), (12, "Nathan"), (13, "Olivia"), (14, "Paul"), (15, "Quincy"), (16, "Ryan"), (17, "Sarah"), (18, "Thomas"), (19, "Uma"), (20, "Victoria"), (21, "William"), (22, "Xavier"), (23, "Yara"), (24, "Zara")]
    # data = [(0, "Alice"), (1, "Bob"), (2, "Carol"), (3, "Dave"), (4, "Eve")]
    for row in data:
        engine.insert_page(row)
    print(engine.buffer_pool.load_page(0).rows)
    # print(engine.get_row(1))
    # print(engine.get_row(2))
    # print(engine.get_row(3))
    # print(engine.get_row(4))
    # engine.insert_page(page=page2)
    # engine.insert_page(page=page3)
    # print(engine.disk.pages[page1.page_id].rows)
    # print(engine.disk.pages[page1.page_id].rows)
    # print(engine.disk.pages[page2.page_id].rows)

if __name__ == "__main__":
    main()