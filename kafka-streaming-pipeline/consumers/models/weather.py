"""Contains functionality related to Weather"""
import json
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        weather_dict = message.value()
        try:
            self.temperature = weather_dict['temperature']
            self.status = weather_dict['status']
            logger.info("extracted Weather(temp=%s, status=%s)", self.temperature, self.status)
        except KeyError as e:
            logger.warning("Failed extract weather data, %s", e)
