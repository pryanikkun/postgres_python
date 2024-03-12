import psycopg2
import config


class ClientDB:
    def __init__(self, connect):
        self.conn = connect
        self.cur = self.conn.cursor()

    def create_tables(self):
        """ Создание таблиц в БД """
        self.cur.execute("""
                CREATE TABLE IF NOT EXISTS client (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(64) NOT NULL,
                last_name VARCHAR(64) NOT NULL,
                email VARCHAR(64) UNIQUE NOT NULL
                );
            """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS phone (
            id SERIAL PRIMARY KEY,
            phone_number DECIMAL(11) UNIQUE NOT NULL,
            client_id INTEGER NOT NULL REFERENCES client(id) ON DELETE CASCADE
            );
        """)

        self.conn.commit()

    def create_client(self, first_name: str, last_name: str,
                      email: str, phones: list = None):
        """
        Добавление нового клиента
        :param first_name: имя клиента
        :param last_name: фамилия клиента
        :param email: почта
        :param phones: список номеров клиента
        """
        self.cur.execute("""
                INSERT INTO client (first_name, last_name, email)
                VALUES (%s, %s, %s) 
                RETURNING id;
            """, (first_name, last_name, email))
        client_id = self.cur.fetchone()[0]
        if phones:
            for phone in phones:
                self.cur.execute("""
                    INSERT INTO phone (phone_number, client_id)
                    VALUES (%s, %s);
                """, (phone, client_id))
        self.conn.commit()

    def add_phone(self, client_id: int, phone: int):
        self.cur.execute("""
            INSERT INTO phone (phone_number, client_id)
            VALUES (%s, %s)
            RETURNING id;
        """, (phone, client_id))
        self.conn.commit()

    def update_client(
            self, client_id: int, first_name: str = None,
            last_name: str = None, email: str = None, phones: list = None):
        """
        Изменение информации о клиенте в БД
        :param client_id: идентификатор клиента
        :param first_name: имя клиента,
        :param last_name: фамилия клиента,
        :param email: новая почта,
        :param phones: номера клиента
        """
        if first_name:
            self.cur.execute("""
                UPDATE client 
                SET first_name=%s
                WHERE id=%s
            """, (first_name, client_id))
        if last_name:
            self.cur.execute("""
                UPDATE client 
                SET last_name=%s
                WHERE id=%s
            """, (last_name, client_id))
        if email:
            self.cur.execute("""
                UPDATE client 
                SET email=%s
                WHERE id=%s
            """, (email, client_id))
        if phones:
            for phone in phones:
                self.cur.execute("""
                    UPDATE phone 
                    SET phone_number=%s
                    WHERE id=%s;
                """, (phone[0], phone[1]))

        self.conn.commit()

    def delete_phone(self, phone: int):
        """
        Удаление телефона для существующего клиента
        :param phone: телефон
        """
        self.cur.execute("""
            DELETE FROM phone WHERE phone_number=%s
        """, (phone,))

        self.conn.commit()

    def delete_client(self, client_id: int):
        """
        Удаление клиента с помощью почты
        :param client_id: идентификатор клиента
        """
        self.cur.execute("""
            DELETE FROM client WHERE id=%s
        """, (client_id,))

        self.conn.commit()

    def get_client(
            self, first_name: str = None, last_name: str = None,
            email: str = None, phone: int = None) -> int:
        """
        Находит клиента по имени, фамилии, почте или телефону
        :param first_name: имя клиента
        :param last_name: фамилия
        :param email: почта
        :param phone: телефон
        :return: кортеж с информацией клиента
        """
        if phone is None:
            self.cur.execute("""
                SELECT *
                FROM client
                WHERE email = %s AND first_name = %s AND last_name = %s;
            """, (email, first_name, last_name))
        else:
            self.cur.execute("""
                SELECT *
                FROM client
                WHERE id=(SELECT client_id
                          FROM phone
                          WHERE phone_number=%s);
            """, (phone,))
        client_id = self.cur.fetchone()
        return client_id

    def get_all_info(self):
        """ Получение всех строк из таблиц"""
        self.cur.execute("""
            SELECT *
            FROM client;
        """)
        print(self.cur.fetchall())
        self.cur.execute("""
            SELECT *
            FROM phone;
        """)
        print(self.cur.fetchall())

    def drop_tables(self):
        """ Сброс всех таблиц """
        self.cur.execute("""
            DROP TABLE phone;
            DROP TABLE client;
        """)
        self.conn.commit()

    def close(self):
        self.conn.close()


if __name__ == '__main__':
    conn = psycopg2.connect(database="manage_client",
                            user=config.POSTGRES_USER,
                            password=config.POSTGRES_PASSWORD)

    my_client = ClientDB(conn)

    #  сброс таблиц
    my_client.drop_tables()

    # создание таблиц
    my_client.create_tables()

    # добавление клиента
    my_client.create_client(first_name='Софья',
                            last_name='Иванова',
                            email='sonya@mail.ru',
                            phones=[79545437777, 79001233322])

    # добавление телефона
    my_client.add_phone(phone=79535433331,
                        client_id=1)

    # изменение информации
    my_client.update_client(client_id=1,
                            first_name='Ирина',
                            last_name='Иванова',
                            email='sonya@mail.ru',
                            phones=[(79001233321, 1), (79535433323, 3)])

    # получение информации о клиенте по телефону
    print(my_client.get_client(phone=79001233322))

    # получение информации о клиенте по данным
    print(my_client.get_client(first_name='Ирина',
                               last_name='Иванова',
                               email='sonya@mail.ru'))
    # удаление телефона
    my_client.delete_phone(phone=79001233322)

    # удаление клиента
    my_client.delete_client(client_id=1)

    # получение всех строк таблиц
    # my_client.get_all_info()

    # закрытие соединения
    my_client.close()



