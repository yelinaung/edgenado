import json
import asyncio
import logging
import signal
import datetime
import edgedb
import tornado.ioloop
import tornado.web
import tornado.log


class MainHandler(tornado.web.RequestHandler):
    def write_json(self, obj, status_code=200):
        self.write(json.dumps(obj))
        self.set_header("Content-Type", "application/json")
        self.set_status(status_code)
        self.finish()

    async def post(self):
        req_body = self.request.body
        json_req_body = None
        try:
            json_req_body = json.loads(req_body)
        except ValueError():
            self.write_json({"error": "invalid_json_body"}, status_code=400)

        first_name = json_req_body.get("first_name")
        last_name = json_req_body.get("last_name")
        db = self.application.client

        result = None
        try:
            result = await db.query(
                """
                INSERT Person {
                    first_name := <str>$first_name,
                    last_name:= <str>$last_name,
                }
                """,
                first_name=first_name,
                last_name=last_name,
            )
        except edgedb.errors.InvalidArgumentError as e:
            self.write_json(
                {"error": "invalid_argument", "additional_data": str(e)},
                status_code=400,
            )
            return
        except edgedb.errors.InvalidValueError as e:
            self.write_json(
                {"error": "invalid_value", "additional_data": str(e)}, status_code=400
            )
            return
        except edgedb.errors.ConstraintViolationError as e:
            self.write_json(
                {"error": "constraint_violation_error", "additional_data": str(e)},
                status_code=400,
            )
            return

        if not result:
            self.write_json(
                {"error": "something_went_wrong!"},
                status_code=400,
            )
            return

        self.write_json(
            {
                "message": "user_created_successfully!",
            }
        )

    async def get(self):
        db = self.application.client
        user_set = await db.query("SELECT Person {first_name, last_name}")
        data = [
            {
                "first_name": user.first_name,
                "last_name": user.last_name,
            }
            for user in user_set
        ]

        self.write_json({"data": data})


class App(tornado.web.Application):
    def __init__(self, **kwargs):
        self.client = edgedb.create_async_client()
        routes = [
            (r"/", MainHandler),
        ]
        super().__init__(routes, **kwargs)

    def start(self, port):
        self.server = self.listen(port)

    async def stop(self, main_loop, signum=None, frame=None):
        """Cleanup tasks tied to the service's shutdown."""
        logging.info(f"Received exit signal {signum.name}...")
        logging.info("Closing database connections")
        await self.client.aclose()

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

        [task.cancel() for task in tasks]

        logging.info(f"Cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks, return_exceptions=True)
        logging.info("Flushing metrics")
        main_loop.stop()


async def run(main_loop):
    try:
        app = App()
        logging.info("Web Started at port 8000")
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            main_loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(app.stop(main_loop, s))
            )
        app.start(8000)
    except Exception:
        tornado.ioloop.IOLoop.instance().stop()
        import traceback

        traceback.print_exc()


def main():
    tornado.log.enable_pretty_logging()
    main_loop = asyncio.get_event_loop()

    try:
        asyncio.ensure_future(run(main_loop))
        main_loop.run_forever()
    except KeyboardInterrupt:
        logging.info("Process interrupted")
    finally:
        main_loop.close()
        logging.info("Successfully shutdown the service.")


if __name__ == "__main__":
    main()
