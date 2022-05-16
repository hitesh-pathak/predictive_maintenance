import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods
import os

class KMeansClustering:
    """
            This class shall  be used to divide the data into clusters before training.

    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def elbow_plot(self, data, filename):
        """
                        Method Name: elbow_plot

                        Description: This method saves the plot to decide the optimum number of clusters to the file.

                        Output: A picture saved to the directory

                        On Failure: Raise Exception

        """
        self.logger_object.log(self.file_object, f'Entered the elbow_plot method of {__class__}')
        wcss=[]  # initializing an empty list
        try:
            self.logger_object.log(self.file_object, 'Plotting elbow plot to identify number of clusters.')
            for i in range (1,11):
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=42)  # initializing the KMeans object
                kmeans.fit(data)  # fitting the data to the KMeans Algorithm
                wcss.append(kmeans.inertia_)

            plt.plot(range(1,11),wcss)  # creating the graph between WCSS and the number of clusters
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            #plt.show()

            self.logger_object.log(self.file_object, "Saving elbow plot to preprocessing data directory.")
            save_path = os.path.join('preprocessing_data/', filename)
            if not os.path.isdir(save_path):
                os.makedirs(save_path)

            plt.savefig(os.path.join(save_path,'K-Means_Elbow.PNG'))  # saving the elbow plot locally
            self.logger_object.log(self.file_object, "Saved plot successfully.")
            # finding the value of the optimum cluster programmatically
            kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.logger_object.log(self.file_object, f'The optimum number of clusters is:{kn.knee}')
            self.logger_object.log(self.file_object, f'Exited the elbow_plot method of {__class__}')

            return kn.knee

        except OSError as ose:
            self.logger_object.log(self.file_object, f'Error{ose}')
            raise ose
        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occurred while finding the number of clusters: {e}')
            self.logger_object.log(self.file_object, f'Exited the elbow_plot method of {__class__}')
            raise e

    def create_clusters(self, data, number_of_clusters, filename):
        """
                                Method Name: create_clusters

                                Description: Create a new dataframe consisting of the cluster information.

                                Output: A datframe with cluster column

                                On Failure: Raise Exception
        """
        self.logger_object.log(self.file_object, f'Entered the create_clusters method of {__class__}')
        try:
            self.logger_object.log(self.file_object, "Creating optimum number of clusters from data.")
            kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)

            y_kmeans = kmeans.fit_predict(data)  # divide data into clusters

            self.logger_object.log(self.file_object, "Saving the KMeans cluster model.")

            file_op = file_methods.File_Operation(self.file_object,self.logger_object, filename)
            file_op.save_model(kmeans, 'KMeans')

            self.logger_object.log(self.file_object, "Save complete. Writing cluster number to data.")
            data['Cluster'] = y_kmeans  # create a new column in dataset for storing the cluster information
            self.logger_object.log(self.file_object, f'Succesfully created {number_of_clusters} clusters.')
            self.logger_object.log(self.file_object, f'Exited the create_clusters method of {__class__}')
            return data

        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occured while creating clusters from data: {e}')
            self.logger_object.log(self.file_object, f'Exited the create_clusters method of {__class__}')
            raise e