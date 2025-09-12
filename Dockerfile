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
# Install dependencies as root to ensure they are in the global site-packages.
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code.
# Copy all Python source files. config.ini is for local dev and is not copied.
COPY --chown=appuser:0 *.py ./

# Also copy the proxy's CA certificate into the container.
COPY --chown=appuser:0 brightdata_ca.pem .

# Now, switch to the non-root user to run the application.
USER appuser

# Run the runner script on container startup.
# This starts the long-running scraping task directly.
CMD ["python", "runner.py"]
