from datetime import datetime, timezone
from typing import List

import numpy as np

from neurons.protocol import ValidationResult


class IncentiveMechanism:
    """Handles reward calculation and distribution."""

    def __init__(self, moving_average_alpha: float = 0.1):
        self.moving_average_alpha = moving_average_alpha
        self.miner_scores = {}
        self.score_history = {}
        self.weights_history = []

    def update_scores(self, validation_results: List[ValidationResult]):
        current_time = datetime.now(timezone.utc)

        for result in validation_results:
            uid = result.uid

            if uid not in self.miner_scores:
                self.miner_scores[uid] = 0.0
                self.score_history[uid] = []

            current_score = self.miner_scores[uid]
            new_score = result.score

            self.miner_scores[uid] = (
                (1 - self.moving_average_alpha) * current_score +
                self.moving_average_alpha * new_score
            )

            self.score_history[uid].append({
                'score': new_score,
                'timestamp': current_time,
                'response_time': result.response_time,
                'success': result.success,
                'confidence': result.confidence
            })

            if len(self.score_history[uid]) > 1000:
                self.score_history[uid] = self.score_history[uid][-1000:]

    def calculate_weights(self, uids: List[int]) -> np.ndarray:
        """Calculate weights for miners based on their scores."""
        if not uids:
            return np.array([])

        weights = np.zeros(len(uids))

        for i, uid in enumerate(uids):
            if uid in self.miner_scores:
                weights[i] = self.miner_scores[uid]
            else:
                weights[i] = 0.0

        temperature = 2.0

        if np.max(weights) > 0:
            exp_weights = np.exp(weights / temperature)
            weights = exp_weights / np.sum(exp_weights)
        else:
            weights = np.ones(len(uids)) / len(uids)

        self.weights_history.append({
            'timestamp': datetime.now(timezone.utc),
            'weights': weights.copy(),
            'uids': uids.copy()
        })

        if len(self.weights_history) > 100:
            self.weights_history = self.weights_history[-100:]

        return weights
