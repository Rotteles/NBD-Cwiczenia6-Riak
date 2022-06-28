package com.example.cw6riak;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

public class CarsInRiak {

    public static class Car {
        public String brand;
        public String version;
        public String horsePower;
        public String productionYear;
    }

    public static class CarUpdate extends UpdateValue.Update<Car> {
        private final Car update;
        public CarUpdate(Car update){
            this.update = update;
        }

        @Override
        public Car apply(Car car) {
            car.brand = update.brand;
            car.version = update.version;
            car.horsePower = update.horsePower;
            car.productionYear = update.productionYear;
            return car;
        }
    }

    private static RiakCluster setUpCluster() {
        RiakNode node = new RiakNode.Builder()
                .withRemoteAddress("localhost")
                .withRemotePort(8087)
                .build();
        RiakCluster cluster = new RiakCluster.Builder(node).build();
        cluster.start();

        return cluster;
    }

    public static void main( String[] args ) {
        try {

            RiakCluster cluster = setUpCluster();
            RiakClient client = new RiakClient(cluster);

            Car audi = new Car();
            audi.brand = "Audi";
            audi.version = "RS3";
            audi.horsePower = "160";
            audi.productionYear = "2008";

            // Add Audi to Riak database
            Namespace carsBucket = new Namespace("cars");
            Location audiCarLocation = new Location(carsBucket, "car_audi");
            StoreValue storeAudiCar = new StoreValue.Builder(audi)
                    .withLocation(audiCarLocation)
                    .build();
            client.execute(storeAudiCar);

            System.out.println("==============================");
            System.out.println("Audi added to database! {" + audi.brand + ", " + audi.version + ", " + audi.horsePower + ", " + audi.productionYear + "}");
            System.out.println("==============================");

            // Fetch Audi from Riak database
            FetchValue fetchAudiCar = new FetchValue.Builder(audiCarLocation)
                    .build();
            Car fetchedAudi = client.execute(fetchAudiCar).getValue(Car.class);

            System.out.println("==============================");
            System.out.println("Audi fetched from database! {" + fetchedAudi.brand + ", " + fetchedAudi.version + ", " + fetchedAudi.horsePower + ", " + fetchedAudi.productionYear + "}");
            System.out.println("==============================");

            // Update Audi in Riak database
            audi.productionYear = "2022";
            CarUpdate updatedAudi = new CarUpdate(audi);
            UpdateValue updateValue = new UpdateValue.Builder(audiCarLocation)
                    .withUpdate(updatedAudi).build();
            client.execute(updateValue);
            Car fetchedAudiAfterUpdate = client.execute(fetchAudiCar).getValue(Car.class);

            System.out.println("==============================");
            System.out.println("Audi updated in database! {" + fetchedAudiAfterUpdate.brand + ", " + fetchedAudiAfterUpdate.version + ", " + fetchedAudiAfterUpdate.horsePower + ", " + fetchedAudiAfterUpdate.productionYear + "}");
            System.out.println("==============================");

            // Delete Audi from Riak database
            DeleteValue deleteAudi = new DeleteValue.Builder(audiCarLocation)
                    .build();
            client.execute(deleteAudi);
            Car fetchedAudiAfterDelete = client.execute(fetchAudiCar).getValue(Car.class);

            System.out.println("==============================");
            System.out.println("Audi deleted from database!");
            System.out.println("==============================");
            System.out.println("{" + fetchedAudiAfterDelete.brand + ", " + fetchedAudiAfterDelete.version + ", " + fetchedAudiAfterDelete.horsePower + ", " + fetchedAudiAfterDelete.productionYear + "}");

            cluster.shutdown();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}

