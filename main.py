from database.db import InnoEngine
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

    engine.insert_page((0, "Alice"))
    engine.insert_page((1, "Bob"))
    engine.insert_page((2, "Carol"))
    engine.insert_page((3, "Dave"))
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