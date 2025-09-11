# Use a stable Python slim base image which is lightweight and secure.
FROM python:3.11-slim-bullseye

# Set environment variables to ensure logs are sent correctly and define the app home.
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME

# Create a non-root user to run the application for better security.
RUN useradd --system --uid 1000 --gid 0 -m -s /bin/bash appuser

# Install Python dependencies.
# Copying requirements first and using --chown improves layer caching and security.
COPY --chown=appuser:0 requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code.
# Copy all Python source files. config.ini is for local dev and is not copied.
COPY --chown=appuser:0 *.py ./

# Switch to the non-root user.
USER appuser

# Run the web service on container startup.
# Use the "exec" form of CMD. This is the most direct and reliable way to start
# the application. The application object is now in 'main.py' at the root of the WORKDIR.
CMD exec gunicorn --bind "0.0.0.0:${PORT:-8080}" --workers 2 --timeout 0 -k uvicorn.workers.UvicornWorker main:app
