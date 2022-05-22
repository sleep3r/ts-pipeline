if __name__ == '__main__':
    import rmq_controller, flask_app, errors_manager

    errors_manager.errors_manager.run()
    rmq_controller.rmq_controller.run()
    flask_app.flask_app.run()