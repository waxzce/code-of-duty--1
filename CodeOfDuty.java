
import java.util.Map.Entry;
import java.util.Map;
import java.util.Iterator;
import akka.routing.Routing.Broadcast;
import akka.actor.UntypedActorFactory;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.routing.CyclicIterator;
import akka.routing.InfiniteIterator;
import akka.routing.UntypedLoadBalancer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.lang.Integer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import scala.collection.immutable.Map.Map1;
import static akka.actor.Actors.actorOf;
import static akka.actor.Actors.poisonPill;
import static java.lang.Math.floor;

/**
 * @author Quentin ADAM @waxzce
 */
public class CodeOfDuty {

   public static void main(String[] args) throws InterruptedException {
      List<String> largs = Arrays.asList(args);
      if (largs.contains("--help")) {
         System.out.println("------------------------------------");
         System.out.println("code by @waxzce");
         System.out.println("This programme is a Vector equalisation system");
         System.out.println("");
         System.out.println("--inputfile inputfile.txt");
         System.out.println("       Change the default input file : input.txt");
         System.out.println("");
         System.out.println("--outputfile outputfile.txt");
         System.out.println("       Change the default output file : output.txt");
         System.out.println("");
         System.out.println("--actorsnb 20");
         System.out.println("       the number of actors computing the file : 20 is the default (very small)");
         System.out.println("");
      } else {
         String inputfilename = "input.txt";
         String outputfilename = "output.txt";
         Integer nbactors = 20;
         if (largs.contains("--inputfile")) {
            inputfilename = largs.get(largs.indexOf("--inputfile") + 1).trim();
            System.out.println("You customize the input file : " + inputfilename);
         }
         if (largs.contains("--outputfile")) {
            outputfilename = largs.get(largs.indexOf("--outputfile") + 1).trim();
            System.out.println("You customize the output file : " + outputfilename);
         }
         if (largs.contains("--actorsnb")) {
            nbactors = Integer.parseInt(largs.get(largs.indexOf("--actorsnb") + 1).trim());
            System.out.println("You customize the number of actors : " + nbactors);
         }
         CodeOfDuty cod = new CodeOfDuty();
         cod.computevectors(inputfilename, outputfilename, nbactors);

      }

   }

   public void computevectors(final String inputfilename, final String outputfilename, final Integer nbactors) throws InterruptedException {
      final CountDownLatch latch = new CountDownLatch(1);

      ActorRef master = actorOf(new UntypedActorFactory() {

         public UntypedActor create() {
            return new Master(latch, nbactors, inputfilename, outputfilename);
         }
      }).start();

      master.sendOneWay(new MasterMessage());

      latch.await();
   }

   static class Master extends UntypedActor {

      private final CountDownLatch latch;
      private Integer nrOfResults;
      private Long start;
      private ActorRef router;
      private Integer computeVectorActorNumber;
      private Integer nbOfComputationSended;
      private Boolean isFinishSend;
      private String inputfilename;
      private String outputfilename;
      private Map<Integer, ComputeVectorMessage> responses;

      static class VectorCalcRouter extends UntypedLoadBalancer {

         private final InfiniteIterator<ActorRef> workers;

         public VectorCalcRouter(ActorRef[] workers) {
            this.workers = new CyclicIterator<ActorRef>(Arrays.asList(workers));
         }

         @Override
         public InfiniteIterator<ActorRef> seq() {
            return workers;
         }
      }

      public Master(CountDownLatch latch, Integer computeVectorActorNumber, String inputfilename, String outputfilename) {
         this.nrOfResults = 0;
         this.latch = latch;
         this.computeVectorActorNumber = computeVectorActorNumber;
         this.inputfilename = inputfilename;
         this.outputfilename = outputfilename;
         this.responses = new TreeMap<Integer, ComputeVectorMessage>();

         final ActorRef[] actors = new ActorRef[computeVectorActorNumber];
         for (int i = 0; i < computeVectorActorNumber; i++) {
            actors[i] = actorOf(VectorCompute.class).start();
         }

         router = actorOf(new UntypedActorFactory() {

            public UntypedActor create() {
               return new VectorCalcRouter(actors);
            }
         }).start();
      }

      // message handler
      public void onReceive(Object message) {

         if (message instanceof MasterMessage) {
            this.isFinishSend = false;

            FileReader fr = null;
            try {
               fr = new FileReader(this.inputfilename);
               LineNumberReader lfr = new LineNumberReader(fr);
               this.nbOfComputationSended = 0;

               while (lfr.ready()) {
                  String line1 = lfr.readLine().trim();

                  if (line1.length() == 1) {
                     Integer nbelem = Integer.parseInt(line1);
                     if (nbelem.equals(0)) {
                        break;
                     }
                     ComputeVectorMessage vm = new ComputeVectorMessage(nbelem, this.nbOfComputationSended);
                     String line2 = lfr.readLine().trim();
                     List<String> lline2 = Arrays.asList(line2.split(" "));
                     for (String sl2 : lline2) {
                        vm.addVectorValue(Integer.parseInt(sl2));
                     }
                     router.sendOneWay(vm, getContext());
                     lfr.readLine();
                     this.nbOfComputationSended++;
                  }
               }
               this.isFinishSend = true;

            } catch (FileNotFoundException ex) {
               Logger.getLogger(CodeOfDuty.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
               Logger.getLogger(CodeOfDuty.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
               try {
                  fr.close();
               } catch (IOException ex) {
                  Logger.getLogger(CodeOfDuty.class.getName()).log(Level.SEVERE, null, ex);
               }
            }





            router.sendOneWay(new Broadcast(poisonPill()));

            router.sendOneWay(poisonPill());

         } else if (message instanceof ComputeVectorMessage) {

            ComputeVectorMessage result = (ComputeVectorMessage) message;

            nrOfResults += 1;

            this.responses.put(result.order, result);

            if (this.nbOfComputationSended.equals(nrOfResults) && this.isFinishSend) {
               getContext().stop();
            }

         } else {
            throw new IllegalArgumentException("Unknown message [" + message + "]");
         }
      }

      @Override
      public void preStart() {
         start = System.currentTimeMillis();
      }

      @Override
      public void postStop() {
         FileWriter fw = null;
         try {
            System.out.println(String.format(
                    "time for compute: \t%s millis",
                    (System.currentTimeMillis() - start)));
            File f = new File(this.outputfilename);
            f.delete();

            fw = new FileWriter(f);

            for (Entry<Integer, ComputeVectorMessage> e : this.responses.entrySet()) {
               fw.write(e.getValue().getPrintable());
               fw.write("\r\n");
            }
            latch.countDown();
         } catch (IOException ex) {
            Logger.getLogger(CodeOfDuty.class.getName()).log(Level.SEVERE, null, ex);
         } finally {
            try {
               fw.flush();
               fw.close();
            } catch (IOException ex) {
               Logger.getLogger(CodeOfDuty.class.getName()).log(Level.SEVERE, null, ex);
            }
         }
      }
   }

   static class MasterMessage {
   }

   static class ComputeVectorMessage {

      private Vector<Integer> vector;
      private List<Vector<Integer>> history;
      private Integer order;
      private Integer avg;
      private Boolean isPossible;
      private String printable;

      public ComputeVectorMessage(Integer nbOfvectorElements, Integer order) {
         vector = new Vector<Integer>(nbOfvectorElements);
         history = new ArrayList<Vector<Integer>>();
         this.order = order;
      }

      public void addVectorValue(Integer i) {
         vector.add(i);
      }

      public Vector<Integer> getVector() {
         return this.vector;
      }

      public Integer getAvg() {
         return avg;
      }

      public void setAvg(Integer avg) {
         this.avg = avg;
      }

      public void pushHistory(Vector<Integer> v) {
         this.history.add(v);
      }

      public List<Vector<Integer>> getHistory() {
         return history;
      }

      public String getPrintable() {
         return printable;
      }

      public void generateTxt() {
         if (isPossible) {
            StringBuilder sb = new StringBuilder();
            sb.append(this.history.size() - 1).append("\r\n");
            for (Integer i = 0; i < this.history.size(); i++) {
               sb.append(i).append(" : (");
               for (Integer ii = 0; ii < this.history.get(i).size() - 1; ii++) {
                  sb.append(this.history.get(i).get(ii)).append(", ");
               }
               sb.append(this.history.get(i).lastElement()).append(")").append("\r\n");
            }
            this.printable = sb.append("\r\n").toString();
         } else {
            this.printable = "-1\r\n";
         }

      }
   }

   static class VectorCompute extends UntypedActor {

      // record the average and test if vector is equalisable
      private ComputeVectorMessage computePossible(ComputeVectorMessage vm) {
         Double d = getlocalAvg(vm.getVector(), vm.getVector().size() - 1);//new Double(sum / vm.getVector().size());
         Double f = floor(d);
         if (vm.isPossible = d.equals(f)) {
            vm.setAvg(f.intValue());
         }
         return vm;
      }

      private Double getlocalAvg(Vector<Integer> v, Integer counter) {
         Double sum = new Double("0");
         for (Integer n = 0; n <= counter; n++) {
            sum += v.get(n);
         }
         return sum / (counter + 1);
      }

      private ComputeVectorMessage computeVector(ComputeVectorMessage vm) {
         Boolean flag = true, oldflag = true;
         Integer counter = -1;
         Integer avg = vm.getAvg();
         Vector<Integer> v = vm.getVector();
         Integer size = v.size();
         Boolean ifavg;
         Double localAVG = new Double(avg);
         vm.pushHistory((Vector) v.clone());
         while (true) {
            localAVG = (counter.equals(-1) ? v.get(0) : getlocalAvg(v, counter));
            counter++;
            oldflag = flag;
            flag = flag && v.get(counter).equals(avg);
            if (flag) {
               if (counter.equals(size - 1)) {
                  vm.pushHistory(v);
                  break;
               } else {
                  continue;
               }
            }

            if (localAVG < avg) {
               if (counter != 0 && !v.get(counter).equals(0)) {
                  v.set(counter, v.get(counter) - 1);
                  v.set(counter - 1, v.get(counter - 1) + 1);

               }
            }
            if (v.get(counter) > avg) {
               if (counter != size - 1 && !v.get(counter).equals(0)) {
                  v.set(counter, v.get(counter) - 1);
                  v.set(counter + 1, v.get(counter + 1) + 1);
               }
            }
            flag = oldflag && v.get(counter).equals(avg);

            if (counter.equals(size - 1)) {
               vm.pushHistory((Vector) v.clone());
               counter = -1;
               flag = true;
               continue;
            }

         }
         Integer s = vm.getHistory().size();
         if (vm.getHistory().get(s - 1).equals(vm.getHistory().get(s - 2))) {
            vm.getHistory().remove(s - 1);
         }

         return vm;
      }

      // message handler
      public void onReceive(Object message) {
         if (message instanceof ComputeVectorMessage) {
            ComputeVectorMessage vm = (ComputeVectorMessage) message;

            vm = computePossible(vm);
            if (vm.isPossible) {
               vm = computeVector(vm);
            }

            vm.generateTxt();
            // reply with the result
            getContext().replyUnsafe(message);

         } else {
            throw new IllegalArgumentException("Unknown message [" + message + "]");
         }
      }
   }
}
