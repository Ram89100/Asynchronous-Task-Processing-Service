# Create your tests here.

from django.test import TestCase
from rest_framework.test import APIClient
from rest_framework import status
from django.contrib.auth.models import User
from datetime import timedelta
from .models import Task 

class TaskTests(TestCase):

    def setUp(self):
        """
        Create a test user and authenticate the APIClient to simulate logged-in user
        """
        self.client = APIClient()
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)  # Automatically authenticate the user

    def test_create_task_success(self):
        data = {'title': 'Test Task', 'duration': '30'}  # Test data for the task creation
        response = self.client.post('/api/tasks/', data)  
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['title'], 'Test Task')

    def test_create_task_missing_fields(self):
        data = {'title': ''}
        response = self.client.post('/api/tasks/', data) 
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('error', response.data)

    def test_get_tasks(self):
        Task.objects.create(title='Task 1', duration=timedelta(minutes=30), user=self.user)
        Task.objects.create(title='Task 2', duration=timedelta(minutes=45), user=self.user)

        response = self.client.get('/api/tasks/list/')  
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

    def test_retrieve_task_success(self):
        task = Task.objects.create(title='Task 1', duration=timedelta(minutes=30), user=self.user)
        response = self.client.get(f'/api/tasks/{task.id}/')  
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['title'], 'Task 1')

    def test_retrieve_task_not_found(self):
        response = self.client.get('/api/tasks/999/')  # Use a non-existent ID
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)

    def test_update_task_success(self):
        task = Task.objects.create(title='Old Title', duration=timedelta(minutes=30), user=self.user)
        data = {'title': 'Updated Title'}
        response = self.client.put(f'/api/tasks/update/{task.id}/', data)  
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('message', response.data)

    def test_update_task_not_found(self):
        data = {'title': 'Updated Title'}
        response = self.client.put('/api/tasks/update/999/', data)  # Use a non-existent ID
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)

    def test_delete_task_success(self):
        task = Task.objects.create(title='Task to Delete', duration=timedelta(minutes=30), user=self.user)
        response = self.client.delete(f'/api/tasks/delete/{task.id}/')  
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

    def test_delete_task_not_found(self):
        response = self.client.delete('/api/tasks/delete/999/')  # Use a non-existent ID
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
