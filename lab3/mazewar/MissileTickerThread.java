public class MissileTickerThread extends Thread {
        public void run(){
                //Enqueue tick event
                try{
                        Thread.sleep(200);
                } catch(InterruptedException e) {
                        e.printStackTrace();
                }
        }
}

